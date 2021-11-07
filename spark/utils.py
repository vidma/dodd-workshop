import os
import shutil



def create_properties_files(trainee):

    prop = """
    trainee=%s

    dam.activity.environment=workshop
    dam.activity.projects=%s_dodd
    dam.activity.explicit.process.name=%s
    dam.activity.explicit.process_run.name=%s
    dam.activity.code.repository=binder://workshop-dodd
    dam.activity.code.version=v1
    dam.activity.organization=Unknown

    mocked.timestamp=%d

    dam.datasources.short.name=true
    dam.datasources.path_rules.short.naming_strategy=
    dam.datasources.short.naming_strategy=File
    dam.logical.datasources.path_rules.naming_strategy=
    dam.logical.datasources.naming_strategy=File

    dam.spark.data_stats.input.computeQuantiles=false
    dam.spark.data_stats.input.cachePerPath=true
    dam.spark.data_stats.input.coalesceEnabled=true
    dam.spark.data_stats.input.coalesceWorkers=1
    dam.spark.data_stats.output.computeQuantiles=false
    dam.spark.data_stats.output.cachePerPath=false
    dam.spark.data_stats.output.coalesceEnabled=false
    dam.spark.data_stats.output.coalesceWorkers=1

    dam.ingestion.is_offline=false
    dam.ingestion.offline.file=entities.log
    dam.ingestion.ignore.ssl.cert=true
    dam.ingestion.entity.compaction=true
    dam.spark.data_stats.enabled=true
    dam.spark.data_stats.input.enabled=true
    dam.spark.data_stats.input.only_used_in_lineage=true
    dam.spark.data_stats.input.keep_filters=true
    dam.spark.shutdown_timeout=600

    dam.spark.file_debug.level=INFO
    dam.spark.file_debug.file_name=debug.log
    dam.spark.file_debug.capture_spark_logs=false

    dam.activity.spark.environnement.provider=io.kensu.dam.lineage.spark.lineage.DefaultSparkEnvironnementProvider
    """

    shutil.move("./data", "./data_"+trainee)

    apps = ["Data Preparation", "KPI"]
    weeks = [1635112800000,1635717600000,1636236000000]
    i = 0
    for app in apps:
        for week in weeks:
            p = prop % (trainee, trainee, app, app, week)
            with open("conf/application%d-week%d.properties" % (apps.index(app)+1, weeks.index(week)+1), "w") as f:
                f.write(p)

