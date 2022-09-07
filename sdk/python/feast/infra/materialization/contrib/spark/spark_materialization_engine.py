import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Literal, Optional, Sequence, Union
import uuid

import dill
import pandas as pd
import pyarrow
from tqdm import tqdm
import pyspark.sql

from feast.batch_feature_view import BatchFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.materialization.batch_materialization_engine import (
    BatchMaterializationEngine,
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
    SparkRetrievalJob,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.passthrough_provider import PassthroughProvider
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.utils import (
    _convert_arrow_to_proto,
    _get_column_names,
    _run_pyarrow_field_mapping,
)

from feast.infra.materialization.local_engine import DEFAULT_BATCH_SIZE

import redis
import time

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='/home/rstudio/des-feast-feature-registry/log_file.log', encoding='utf-8',level=logging.DEBUG)

class SparkMaterializationEngineConfig(FeastConfigBaseModel):
    """Batch Materialization Engine config for spark engine"""

    type: Literal["spark"] = "spark"
    """ Type selector"""


@dataclass
class SparkMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        status: MaterializationJobStatus,
        error: Optional[BaseException] = None,
    ) -> None:
        super().__init__()
        self._job_id: str = job_id
        self._status: MaterializationJobStatus = status
        self._error: Optional[BaseException] = error

    def status(self) -> MaterializationJobStatus:
        return self._status

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return self._job_id

    def url(self) -> Optional[str]:
        return None


class SparkMaterializationEngine(BatchMaterializationEngine):
    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        # Nothing to set up.
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        # Nothing to tear down.
        pass

    def __init__(
        self,
        *,
        repo_config: RepoConfig,
        offline_store: SparkOfflineStore,
        online_store: OnlineStore,
        **kwargs,
    ):
        if not isinstance(offline_store, SparkOfflineStore):
            raise TypeError(
                "SparkMaterializationEngine is only compatible with the SparkOfflineStore"
            )
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )

    def materialize(
        self, registry, tasks: List[MaterializationTask]
    ) -> List[MaterializationJob]:
        return [
            self._materialize_one(
                registry,
                task.feature_view,
                task.start_time,
                task.end_time,
                task.project,
                task.tqdm_builder,
            )
            for task in tasks
        ]

    def _materialize_one(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ):
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        job_id = f"{feature_view.name}-{start_date}-{end_date}"

        try:
            offline_job: SparkRetrievalJob = (
                self.offline_store.pull_latest_from_table_or_query(
                    config=self.repo_config,
                    data_source=feature_view.batch_source,
                    join_key_columns=join_key_columns,
                    feature_name_columns=feature_name_columns,
                    timestamp_field=timestamp_field,
                    created_timestamp_column=created_timestamp_column,
                    start_date=start_date,
                    end_date=end_date,
                )
            )

            spark_serialized_artifacts = _SparkSerializedArtifacts.serialize(
                feature_view=feature_view, repo_config=self.repo_config
            )

            spark_df: pyspark.sql.DataFrame = offline_job.to_spark_df()
            
            n = spark_df.cache().count()
            k = len(spark_df.columns)
            
            
            target = 300000
            batch_size = int(target/k) # 930
            partitions = int(n/batch_size)+1

            print(f"{n} rows will be written to the online store in {partitions:,} batches of size {batch_size:,}")
            
            spark_df.rdd.repartition(partitions).foreachPartition(
                lambda x: _process_by_partition(x, spark_serialized_artifacts)
            )

            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )


@dataclass
class _SparkSerializedArtifacts:
    """Class to assist with serializing unpicklable artifacts to the spark workers"""

    feature_view_proto: str
    repo_config_file: str

    @classmethod
    def serialize(cls, feature_view, repo_config):

        # serialize to proto
        feature_view_proto = feature_view.to_proto().SerializeToString()

        # serialize repo_config to disk. Will be used to instantiate the online store
        repo_config_file = tempfile.NamedTemporaryFile(delete=False).name
        with open(repo_config_file, "wb") as f:
            dill.dump(repo_config, f)

        return _SparkSerializedArtifacts(
            feature_view_proto=feature_view_proto, repo_config_file=repo_config_file
        )

    def unserialize(self):
        # unserialize
        proto = FeatureViewProto()
        proto.ParseFromString(self.feature_view_proto)
        feature_view = FeatureView.from_proto(proto)

        # load
        with open(self.repo_config_file, "rb") as f:
            repo_config = dill.load(f)

        provider = PassthroughProvider(repo_config)
        online_store = provider.online_store
        return feature_view, online_store, repo_config


def _process_by_partition(rows, spark_serialized_artifacts: _SparkSerializedArtifacts):
    """Load pandas df to online store"""

    name = str(uuid.uuid4())
    t0 = time.time()

    # convert to pyarrow table
    dicts = [row.asDict() for row in rows]

    df = pd.DataFrame.from_records(dicts)
    if df.shape[0] == 0:
        print("Skipping")
        return

    rows, columns = df.shape
    records = rows*columns

    msg = f"{name}: {rows:} x {columns:}: Start"
    logging.info(msg)

    table = pyarrow.Table.from_pandas(df)


    # unserialize artifacts
    feature_view, online_store, repo_config = spark_serialized_artifacts.unserialize()

    if feature_view.batch_source.field_mapping is not None:
        table = _run_pyarrow_field_mapping(
            table, feature_view.batch_source.field_mapping
        )

    join_key_to_value_type = {
        entity.name: entity.dtype.to_value_type()
        for entity in feature_view.entity_columns
    }
    batch = table
    
    rows_to_write = _convert_arrow_to_proto(
        batch, feature_view, join_key_to_value_type
    )
    online_store.online_write_batch(
        repo_config,
        feature_view,
        rows_to_write,
        None,
    )
    t1 = time.time()
    msg = f"{name}: {rows:} x {columns:}: End: {t1-t0:,.2f}"
    logger.info(msg)
        

    # rows_to_write = _convert_arrow_to_proto(table, feature_view, join_key_to_value_type)

    # path = f"/home/rstudio/des-feast-feature-registry/data/{str(uuid.uuid4())}.parquet"
    # os.makedirs(os.path.dirname(path),exist_ok=True)
    # df.to_parquet(path)

    

    # online_store.online_write_batch(
    #     repo_config,
    #     feature_view,
    #     rows_to_write,
    #     lambda x: None,
    # )
