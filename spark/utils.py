import os
import shutil



def create_properties_files(trainee, api_url = "", ingestion_token = "",pat="", sdk_url ="", offline= False):
    
    if not offline and (api_url == "" or ingestion_token == ""):
        raise Exception("Missing api_url or token")
        
    
    if (api_url == "" and ingestion_token != "") or (api_url != "" and ingestion_token == ""):
        raise Exception("Missing PAT or SDK_URL")
    
        

    prop = """
    
    dam.activity.user=%s
    
    dam.ingestion.url=%s
    dam.ingestion.auth.token=%s
    
    kensu.pat=%s
    kensu.sdk.url=%s
    
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

    dam.ingestion.is_offline=%s
    dam.ingestion.offline.file=entities.log
    dam.ingestion.ignore.ssl.cert=true
    dam.ingestion.entity.compaction=true
    dam.spark.data_stats.enabled=true
    dam.spark.data_stats.input.enabled=true
    dam.spark.data_stats.input.only_used_in_lineage=true
    dam.spark.data_stats.input.keep_filters=false
    dam.spark.shutdown_timeout=600

    dam.spark.file_debug.level=INFO
    dam.spark.file_debug.file_name=debug.log
    dam.spark.file_debug.capture_spark_logs=false

    dam.activity.spark.environnement.provider=io.kensu.dam.lineage.spark.lineage.DefaultSparkEnvironnementProvider
    """


    apps = ["Data Preparation", "KPI"]
    weeks = [1635112800000,1635717600000,1636236000000,1636934400000]
    i = 0
    for app in apps:
        for week in weeks:
            p = prop % (trainee, api_url, ingestion_token, pat, sdk_url, trainee, app, app+trainee, week, str(offline).lower())
            with open("conf/application%d-week%d.properties" % (apps.index(app)+1, weeks.index(week)+1), "w") as f:
                f.write(p)




