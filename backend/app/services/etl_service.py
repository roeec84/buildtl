import os
import sys
import requests
from typing import List, Dict, Any, Optional, Tuple
from pyspark.sql import SparkSession
from app.core.config import settings

class ETLService:
    _spark = None

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        """
        Get or create the Spark Session.
        Ensure JDBC drivers are available.
        """
        if cls._spark is None:
            if sys.platform == "darwin":
                os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
                os.environ["no_proxy"] = "*"  # Fix for some macOS network issues

            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
            
            # Debug: Print environment info
            print(f"DEBUG: Initializing Spark Session")
            print(f"DEBUG: Python: {sys.executable}")
            print(f"DEBUG: Java Home: {os.environ.get('JAVA_HOME', 'Not Set')}")
            print(f"DEBUG: CWD: {os.getcwd()}")

            # Ensure drivers exist
            driver_path = cls._ensure_drivers()
            
            # Build session
            # Note: In production, i might submit jobs to a cluster.
            # Here i run local mode.
            builder = SparkSession.builder \
                .appName("BuildTL") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .config("spark.jars", driver_path) \
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
                .config("spark.hadoop.fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
            
            cls._spark = builder.getOrCreate()
            
        return cls._spark

    @classmethod
    def _ensure_drivers(cls) -> str:
        """
        Download necessary JDBC drivers if missing.
        Returns the path to the jars (comma separated).
        """
        driver_dir = os.path.join(os.getcwd(), "drivers")
        os.makedirs(driver_dir, exist_ok=True)
        
        jars = []
        
        # PostgreSQL Driver
        pg_jar_name = "postgresql-42.7.2.jar"
        pg_jar = os.path.join(driver_dir, pg_jar_name)
        if not os.path.exists(pg_jar):
            print("Downloading PostgreSQL JDBC Driver...")
            url = f"https://jdbc.postgresql.org/download/{pg_jar_name}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                with open(pg_jar, "wb") as f:
                    f.write(response.content)
            except Exception as e:
                print(f"Failed to download driver: {e}")
        jars.append(pg_jar)
        
        # BigQuery Connector
        # Using 0.41.0 which supports Spark 3.5
        bq_jar_name = "spark-bigquery-with-dependencies_2.12-0.41.0.jar"
        bq_jar = os.path.join(driver_dir, bq_jar_name)
        if not os.path.exists(bq_jar):
             print(f"Downloading BigQuery Connector ({bq_jar_name})...")
             # Use a reliable maven repo link
             url = f"https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.41.0/{bq_jar_name}"
             try:
                 response = requests.get(url)
                 response.raise_for_status()
                 with open(bq_jar, "wb") as f:
                     f.write(response.content)
                 print("BigQuery driver downloaded successfully.")
             except Exception as e:
                 print(f"Failed to download BigQuery driver: {e}")
        jars.append(bq_jar)

        # GCS Connector (Required for BigQuery writes)
        # Using 2.2.22 shaded which bundles dependencies
        gcs_jar_name = "gcs-connector-hadoop3-2.2.22-shaded.jar"
        gcs_jar = os.path.join(driver_dir, gcs_jar_name)
        if not os.path.exists(gcs_jar):
            print(f"Downloading GCS Connector ({gcs_jar_name})...")
            url = f"https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/{gcs_jar_name}"
            try:
                response = requests.get(url)
                response.raise_for_status()
                with open(gcs_jar, "wb") as f:
                    f.write(response.content)
                print("GCS connector downloaded successfully.")
            except Exception as e:
                print(f"Failed to download GCS connector: {e}")
        jars.append(gcs_jar)

        # AWS S3 Support (Hadoop AWS + AWS SDK Bundle)
        # Compatible with Spark 3.5 / Hadoop 3.3.4
        hadoop_aws_jar = os.path.join(driver_dir, "hadoop-aws-3.3.4.jar")
        aws_sdk_jar = os.path.join(driver_dir, "aws-java-sdk-bundle-1.12.262.jar")
        
        if not os.path.exists(hadoop_aws_jar):
            print("Downloading Hadoop AWS jar...")
            url = f"https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
            try:
                requests.get(url, stream=True).raise_for_status()
                with open(hadoop_aws_jar, "wb") as f:
                    f.write(requests.get(url).content)
            except Exception as e:
                print(f"Failed to download Hadoop AWS: {e}")
        jars.append(hadoop_aws_jar)
        
        if not os.path.exists(aws_sdk_jar):
            print("Downloading AWS SDK Bundle...")
            url = f"https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
            try:
                requests.get(url, stream=True).raise_for_status()
                with open(aws_sdk_jar, "wb") as f:
                    f.write(requests.get(url).content)
            except Exception as e:
                print(f"Failed to download AWS SDK: {e}")
        jars.append(aws_sdk_jar)
        jars.append(aws_sdk_jar)

        # Azure Data Lake Gen2 Support (Hadoop Azure)
        hadoop_azure_jar = os.path.join(driver_dir, "hadoop-azure-3.3.4.jar")
        # Azure needs azure-storage-blob or similar. Usually part of sdk bundle or separate.
        # For simplicity, we'll try to rely on what's available or download minimal.
        # Ideally we need: hadoop-azure + azure-storage
        azure_storage_jar = os.path.join(driver_dir, "azure-storage-8.6.6.jar")

        if not os.path.exists(hadoop_azure_jar):
             print("Downloading Hadoop Azure...")
             url = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar"
             try:
                 requests.get(url, stream=True).raise_for_status()
                 with open(hadoop_azure_jar, "wb") as f:
                     f.write(requests.get(url).content)
             except Exception as e:
                 print(f"Failed to download Hadoop Azure: {e}")
        jars.append(hadoop_azure_jar)
        
        if not os.path.exists(azure_storage_jar):
             print("Downloading Azure Storage SDK...")
             url = "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar"
             try:
                 requests.get(url, stream=True).raise_for_status()
                 with open(azure_storage_jar, "wb") as f:
                     f.write(requests.get(url).content)
             except Exception as e:
                 print(f"Failed to download Azure Storage SDK: {e}")
        jars.append(azure_storage_jar)
        return ",".join(jars)

    @staticmethod
    def _decrypt_config(config: dict) -> dict:
        """
        Decrypt sensitive fields in the configuration dictionary.
        """
        from app.core.security import decrypt_value
        config = config.copy()
        sensitive_keys = ["password", "secret_key", "account_key", "access_key", "credentials_json"]
        
        def is_encrypted(s):
            return isinstance(s, str) and s.startswith('gAAAA')

        for key in sensitive_keys:
            if key in config and is_encrypted(config[key]):
                config[key] = decrypt_value(config[key])
        return config

    @staticmethod
    def _build_connection_string(db_type: str, config: dict) -> str:
        """
        Build SQLAlchemy connection string from config.
        """
        if db_type == 'postgresql':
            return f"postgresql://{config.get('username')}:{config.get('password')}@{config.get('host')}:{config.get('port')}/{config.get('database')}"
        elif db_type == 'mysql':
            return f"mysql+pymysql://{config.get('username')}:{config.get('password')}@{config.get('host')}:{config.get('port')}/{config.get('database')}"
        elif db_type in ['sql_server', 'azure_sql']:
            return f"mssql+pymysql://{config.get('username')}:{config.get('password')}@{config.get('host')}:{config.get('port')}/{config.get('database')}"
        return ""

    @staticmethod
    def _get_jdbc_url(db_type: str, config: dict) -> str:
        """
        Construct JDBC URL from configuration.
        """
        connection_string = ETLService._build_connection_string(db_type, config)
        if db_type == 'postgresql':
            return connection_string.replace('postgresql://', 'jdbc:postgresql://')
        elif db_type == 'mysql':
            return connection_string.replace('mysql+pymysql://', 'jdbc:mysql://')
        elif db_type in ['sql_server', 'azure_sql']:
            return connection_string.replace('mssql+pyodbc://', 'jdbc:sqlserver://')
        return connection_string

    @staticmethod
    def _configure_cloud_storage(sc, db_type: str, config: dict) -> None:
        """
        Configure Spark Hadoop/FileSystem context for cloud storage.
        """
        conf = sc._jsc.hadoopConfiguration()
        
        if db_type in ['s3', 'minio']:
            if 'access_key' in config: conf.set("fs.s3a.access.key", config['access_key'])
            if 'secret_key' in config: conf.set("fs.s3a.secret.key", config['secret_key'])
            if 'endpoint' in config:
                 conf.set("fs.s3a.endpoint", config['endpoint'])
                 conf.set("fs.s3a.path.style.access", "true")

        elif db_type == 'gcs':
            if 'credentials_json' in config:
                 import json
                 creds = json.loads(config['credentials_json'])
                 conf.set("fs.gs.auth.service.account.email", creds.get('client_email'))
                 conf.set("fs.gs.auth.service.account.private.key", creds.get('private_key'))
                 conf.unset("fs.gs.auth.service.account.json.keyfile")
                 conf.set("fs.gs.auth.service.account.private.key.id", creds.get('private_key_id'))
                 conf.set("google.cloud.auth.service.account.enable", "true")

        elif db_type == 'adls':
            account_name = config.get('account_name')
            account_key = config.get('account_key')
            if account_name and account_key:
                conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)

    @staticmethod
    def _normalize_path(db_type: str, path: str, config: dict) -> str:
        """
        Normalize cloud storage paths to include protocol and bucket/container.
        """
        path = path.lstrip('/') if path else ""
        
        if db_type in ['s3', 'minio']:
            if not path.startswith("s3a://"):
                bucket = config.get('bucket')
                if bucket and not path.startswith(bucket):
                     return f"s3a://{bucket}/{path}"
                return f"s3a://{path}"
        
        elif db_type == 'gcs':
            if not path.startswith("gs://"):
                bucket = config.get('bucket')
                if bucket and not path.startswith(bucket):
                     return f"gs://{bucket}/{path}"
                return f"gs://{path}"
        
        elif db_type == 'adls':
            account_name = config.get('account_name')
            if not path.startswith("abfss://"):
                container = config.get('container')
                if container and not path.startswith(container):
                     return f"abfss://{container}@{account_name}.dfs.core.windows.net/{path}"
                # Assume full container path or just container
                path_suffix = f"@{account_name}.dfs.core.windows.net"
                if path_suffix not in path:
                     return f"abfss://{path}{path_suffix}"
                return f"abfss://{path}"
                
        return path

    @staticmethod
    def load_source_data(datasource, selected_columns, limit=None):
        """
        Load data from a source datasource with selected columns.
        Returns a Spark DataFrame.
        """
        from pyspark.sql import SparkSession
        import base64
        
        spark = ETLService.get_spark_session()
        
        # Access Linked Service
        ls = datasource.linked_service
        if not ls:
             raise ValueError("Linked Service not found for data source")
             
        db_type = ls.service_type
        # Helper: Decrypt config
        config = ETLService._decrypt_config(ls.connection_config)
        
        # JDBC Sources
        if db_type in ['postgresql', 'mysql', 'sql_server', 'azure_sql']:
            jdbc_url = ETLService._get_jdbc_url(db_type, config)
            
            df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", datasource.table_name) \
                .option("user", config.get('username', '')) \
                .option("password", config.get('password', '')) \
                .load()
        
        elif db_type == 'bigquery':
            project_id = config.get('project_id')
            dataset_id = config.get('dataset_id')
            table_id = datasource.table_name
            
            # Base64 encode the JSON
            creds_b64 = base64.b64encode(config['credentials_json'].encode('utf-8')).decode('utf-8')
            
            full_table_id = f"{dataset_id}.{table_id}"
            if project_id:
                full_table_id = f"{project_id}.{full_table_id}"

            reader = spark.read \
                .format("bigquery") \
                .option("viewsEnabled", "true") \
                .option("materializationDataset", dataset_id) \
                .option("credentials", creds_b64)
            
            if project_id:
                 reader = reader.option("parentProject", project_id)

            df = reader.load(full_table_id)
            
        elif db_type in ['s3', 'minio', 'gcs', 'adls']:
            # Configure FileSystem
            sc = spark.sparkContext
            ETLService._configure_cloud_storage(sc, db_type, config)
            
            # Normalize Path
            path = ETLService._normalize_path(db_type, datasource.table_name, config)
            
            # Determine format
            fmt = "parquet"
            if path.endswith(".csv"): fmt = "csv"
            elif path.endswith(".json"): fmt = "json"
            elif path.endswith(".txt"): fmt = "text"
            
            reader = spark.read.format(fmt)
            if fmt == "csv": reader = reader.option("header", "true").option("inferSchema", "true")
            df = reader.load(path)

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        # Common post-processing
        if selected_columns:
            df = df.select(*selected_columns)
        
        if limit:
            df = df.limit(limit)
            
        return df

    @staticmethod
    async def test_connection(db_type: str, connection_config: dict, table_name: str = None) -> tuple[bool, str]:
        """
        Test connection to a datasource configuration.
        Returns (success, message).
        """
        try:
            spark = ETLService.get_spark_session()
            # Helper: Decrypt config
            config = ETLService._decrypt_config(connection_config)
            
            if db_type in ['postgresql', 'mysql', 'sql_server', 'azure_sql']:
                jdbc_url = ETLService._get_jdbc_url(db_type, config)
                target_table = table_name if table_name else "(SELECT 1) as test_connection"
                
                spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", target_table) \
                    .option("user", config.get('username', '')) \
                    .option("password", config.get('password', '')) \
                    .load() \
                    .limit(1).collect()
                    
            elif db_type in ['s3', 'minio', 'gcs', 'adls']:
                # Configure Cloud Storage
                sc = spark.sparkContext
                ETLService._configure_cloud_storage(sc, db_type, config)
                
                test_path = table_name if table_name else ""
                # Normalize Path
                # If no table name (just testing bucket), _normalize_path handles empty path intelligently? 
                # Let's check. Yes, it returns just the root like "s3a://bucket/" if path is empty.
                full_test_path = ETLService._normalize_path(db_type, test_path, config)

                # Validate connection
                if table_name:
                     fmt = "parquet"
                     if table_name.endswith(".csv"): fmt = "csv"
                     elif table_name.endswith(".json"): fmt = "json"
                     elif table_name.endswith(".txt"): fmt = "text"
                     spark.read.format(fmt).load(full_test_path).limit(1).collect()
                else:
                     # Check bucket access
                     try:
                         Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
                         conf = sc._jsc.hadoopConfiguration()
                         fs = Path(full_test_path).getFileSystem(conf)
                         fs.listStatus(Path(full_test_path))
                     except Exception as e:
                         raise Exception(f"Failed to access bucket/container: {str(e)}")
                
            elif db_type == 'bigquery':
                # Check auth by just listing datasets or doing a dry run? 
                # For now, just loading nothing or similar. 
                # User previous implementation just called collect() on empty DF?
                # No, it did ".limit(1).collect()". 
                # Ideally we should try to read a dummy query.
                import base64
                project_id = config.get('project_id')
                dataset_id = config.get('dataset_id')
                
                creds_b64 = base64.b64encode(config['credentials_json'].encode('utf-8')).decode('utf-8')
                reader = spark.read \
                    .format("bigquery") \
                    .option("viewsEnabled", "true") \
                    .option("materializationDataset", dataset_id) \
                    .option("credentials", creds_b64)
                
                if project_id:
                     reader = reader.option("parentProject", project_id)
                
                # Run simple query to test connection
                reader.option("query", "SELECT 1").load().limit(1).collect()
                
            else:
                return False, f"Unsupported database type: {db_type}"

            return True, "Connection successful"
            
        except Exception as e:
            return False, str(e)

    @staticmethod
    async def get_table_schema(datasource, db_session=None) -> list:
        """
        Get schema for a datasource.
        Returns list of dicts: {'name': str, 'type': str, 'nullable': bool}
        """
        try:
            df = ETLService.load_source_data(datasource, selected_columns=None, limit=1)
            
            return [
                {
                    'name': field.name,
                    'type': str(field.dataType).replace('()', ''),
                    'nullable': field.nullable
                }
                for field in df.schema.fields
            ]
        except Exception as e:
            raise Exception(f"Failed to get table schema: {str(e)}")
    
    @staticmethod
    async def preview_transformation(
        sources: list,
        transformation_prompt: str,
        db_session,
        user_id: int,
        limit: int = 1000,
        model_name: str = "gpt-4o"
    ):
        """
        Execute transformation and return preview data.
        Returns dict with columns, data, row_count, and generated_code.
        """
        from sqlalchemy import select
        from sqlalchemy.orm import joinedload
        from app.models.etl import ETLDataSource
        from app.models.settings import ModelSetting
        
        input_dfs = {}
        upstream_schemas = {}
        
        # Load all source data
        for source in sources:
            source_id = source.datasource_id
            selected_cols = source.selected_columns
            table_name = source.table_name
            
            # Fetch source datasource
            result = await db_session.execute(
                select(ETLDataSource).options(joinedload(ETLDataSource.linked_service)).where(ETLDataSource.id == source_id)
            )
            datasource = result.scalar_one_or_none()
            
            if not datasource:
                raise ValueError(f"Data source {source_id} not found")
            
            try:
                # Fetch full dataset (lazy)
                df = ETLService.load_source_data(datasource, selected_cols, limit=None)
                input_dfs[table_name] = df
                upstream_schemas[table_name] = {field.name: str(field.dataType) for field in df.schema.fields}
                print(f"DEBUG: Loaded source {table_name}")
            except Exception as e:
                raise Exception(f"Failed to load source data for {table_name}: {str(e)}")

        # Fetch user's API key
        api_key = None
        try:
            result = await db_session.execute(
                select(ModelSetting)
                .where(ModelSetting.user_id == user_id)
                .where(ModelSetting.name == model_name)
            )
            model_setting = result.scalar_one_or_none()
            if model_setting and model_setting.api_key:
                from app.core.security import decrypt_value
                api_key = decrypt_value(model_setting.api_key)
        except Exception as e:
            print(f"Warning: Failed to fetch user API key: {e}")

        # Generate transformation code
        try:
            generated_code = await ETLService.generate_transformation_code(
                transformation_prompt,
                upstream_schemas,
                model_name=model_name,
                api_key=api_key
            )
        except Exception as e:
            raise Exception(f"Failed to generate transformation code: {str(e)}")
        
        # Execute transformation
        try:
            # Create execution namespace
            namespace = {
                'F': __import__('pyspark.sql.functions', fromlist=['*']),
                'T': __import__('pyspark.sql.types', fromlist=['*']),
            }
            
            # Execute generated code
            print(f"DEBUG: Executing generated code:\n{generated_code}")
            exec(generated_code, namespace)
            
            if 'transform' not in namespace:
                raise ValueError("Generated code must define a 'transform' function")
            
            transform_func = namespace['transform']
            
            # Pass input_dfs dictionary to transform function
            spark = ETLService.get_spark_session()
            transformed_df = transform_func(spark, input_dfs)
            
            print(f"DEBUG: Transformation executed. Result count: {transformed_df.count()}")
            
        except Exception as e:
            print(f"DEBUG: Error executing transformation: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Failed to execute transformation: {str(e)}")
        
        # Collect preview data
        try:
            preview_rows = transformed_df.limit(limit).collect()
            columns = transformed_df.columns
            data = [[row[col] for col in columns] for row in preview_rows]
            row_count = len(preview_rows)
            
            return {
                'columns': columns,
                'data': data,
                'row_count': row_count,
                'generated_code': generated_code,
                'source_schema': upstream_schemas
            }
        except Exception as e:
            raise Exception(f"Failed to collect preview data: {str(e)}")
    
    @staticmethod
    def write_sink_data(df, datasource, table_name, mode='append'):
        """
        Write DataFrame to sink datasource.
        """
        import base64
        
        ls = datasource.linked_service
        if not ls:
             raise ValueError("Linked Service not found for sink datasource")
        db_type = ls.service_type
        # Helper: Decrypt config
        config = ETLService._decrypt_config(ls.connection_config)
            
        if db_type in ['postgresql', 'mysql', 'sql_server', 'azure_sql']:
            jdbc_url = ETLService._get_jdbc_url(db_type, config)
            
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", config.get('username', '')) \
                .option("password", config.get('password', '')) \
                .mode(mode) \
                .save()
                
        elif db_type == 'bigquery':
            project_id = config.get('project_id')
            dataset_id = config.get('dataset_id')
            
            # Credentials handling (Base64)
            creds_b64 = base64.b64encode(config['credentials_json'].encode('utf-8')).decode('utf-8')
            
            # Configure Hadoop config for GCS (used internally by BQ writes)
            try:
                sc = df.sparkSession.sparkContext
                ETLService._configure_cloud_storage(sc, 'gcs', config)
            except Exception as e:
                print(f"Warning: Failed to set Hadoop configuration: {e}")

            full_table_id = f"{dataset_id}.{table_name}"
            if project_id:
                full_table_id = f"{project_id}.{full_table_id}"
            
            # Use Direct Write method
            writer = df.write \
                .format("bigquery") \
                .option("credentials", creds_b64) \
                .option("writeMethod", "direct")
            
            if project_id:
                writer = writer.option("parentProject", project_id)
            
            writer.mode(mode).save(full_table_id)

        elif db_type in ['s3', 'minio', 'gcs', 'adls']:
            # Configure FileSystem
            sc = df.sparkSession.sparkContext
            ETLService._configure_cloud_storage(sc, db_type, config)
            
            # Normalize Path
            path = ETLService._normalize_path(db_type, table_name, config)
            
            # Determine format
            fmt = "parquet"
            if path.endswith(".csv"): fmt = "csv"
            elif path.endswith(".json"): fmt = "json"
            elif path.endswith(".txt"): fmt = "text"
            
            writer = df.write.format(fmt).mode(mode)
            if fmt == "csv": writer = writer.option("header", "true")
            writer.save(path)
            
        else:
            raise ValueError(f"Unsupported sink database type: {db_type}")

    @staticmethod
    async def get_sqlalchemy_engine(datasource_id: int, db_session):
        """
        Get a SQLAlchemy Engine for a given datasource.
        Avoids writing secrets to disk by passing credentials in memory where possible.
        """
        from sqlalchemy import select, create_engine
        from sqlalchemy.orm import joinedload
        from app.models.etl import ETLDataSource
        from app.core.security import decrypt_value
        import json

        # Fetch datasource
        result = await db_session.execute(
            select(ETLDataSource).options(joinedload(ETLDataSource.linked_service)).where(ETLDataSource.id == datasource_id)
        )
        datasource = result.scalar_one_or_none()
        if not datasource:
            raise ValueError(f"Datasource {datasource_id} not found")

        ls = datasource.linked_service
        if not ls:
            raise ValueError("Linked Service not found for data source")

        db_type = ls.service_type
        config = ETLService._decrypt_config(ls.connection_config)

        if db_type in ['postgresql', 'mysql', 'sql_server', 'azure_sql']:
            url = ETLService._build_connection_string(db_type, config)
            return create_engine(url)

        elif db_type == 'bigquery':
            import json
            try:
                creds_dict = json.loads(config['credentials_json'])
                url = f"bigquery://{config.get('project_id')}"
                return create_engine(url, connect_args={'credentials_info': creds_dict})
            except Exception as e:
                print(f"Error handling BigQuery credentials: {e}")
                return create_engine(f"bigquery://{config.get('project_id')}")

        elif db_type in ['s3', 'minio', 'gcs', 'adls']:
            # For file-based sources, we return a configuration dict
            # We also infer schema to help the LLM
            schema_info = "Schema unavailable"
            try:
                spark = ETLService.get_spark_session()
                sc = spark.sparkContext
                ETLService._configure_cloud_storage(sc, db_type, config)
                path = ETLService._normalize_path(db_type, datasource.table_name, config)
                
                fmt = "parquet"
                if path.endswith(".csv"): fmt = "csv"
                elif path.endswith(".json"): fmt = "json"
                elif path.endswith(".txt"): fmt = "text"
                
                reader = spark.read.format(fmt)
                if fmt == "csv": reader = reader.option("header", "true").option("inferSchema", "true")
                
                # Load schema only (limit 1)
                df = reader.load(path).limit(1)
                schema_info = "\n".join([f"{f.name}: {f.dataType}" for f in df.schema.fields])
                
            except Exception as e:
                print(f"Warning: Failed to infer schema for {db_type} source: {e}")

            # Sanitize table name for Spark View (alphanumeric only usually)
            view_name = datasource.table_name.replace(".", "_").replace("/", "_").replace("-", "_")

            return {
                'type': db_type,
                'table_name': datasource.table_name,
                'view_name': view_name,
                'config': config,
                'is_file_source': True,
                'schema_info': schema_info
            }
        else:
            raise ValueError(f"Unsupported database type for SQL Agent: {db_type}")

    @staticmethod
    async def execute_sql_query(source, query: str) -> List[Dict[str, Any]]:
        """
        Execute SQL query using SQLAlchemy Engine (DBs) or Spark (Files).
        Args:
            source: SQLAlchemy Engine OR a Dictionary containing file source config
            query: SQL query string
        """
        import pandas as pd
        from sqlalchemy.engine.base import Engine
        
        # 1. Handle SQLAlchemy Engine
        if isinstance(source, Engine):
            try:
                with source.connect() as conn:
                    df = pd.read_sql(text(query), conn)
                return df.astype(object).where(pd.notnull(df), None).to_dict('records')
            except Exception as e:
                print(f"Error executing SQL: {e}")
                raise e

        # 2. Handle Cloud Buckets (Spark)
        elif isinstance(source, dict) and source.get('is_file_source'):
            try:
                from pyspark.sql import SparkSession
                spark = ETLService.get_spark_session()
                
                # Mock datasource objects to reuse load_source_data
                class MockLinkedService:
                    def __init__(self, service_type, connection_config):
                        self.service_type = service_type
                        self.connection_config = connection_config
                
                class MockDatasource:
                    def __init__(self, table_name, linked_service):
                        self.table_name = table_name
                        self.linked_service = linked_service
                
                # Reconstruct mock objects
                # Note: config is already decrypted in get_sqlalchemy_engine, but
                # load_source_data attempts to decrypt again. decrypt_value handles plaintext gracefully? 
                # Our _decrypt_config helper checks for 'gAAAA' prefix, so it's safe to pass already decrypted data.
                mock_ls = MockLinkedService(source['type'], source['config'])
                mock_ds = MockDatasource(source['table_name'], mock_ls)
                
                # Load DataFrame
                df = ETLService.load_source_data(mock_ds, selected_columns=None)
                
                # Register Temp View for SQL access
                view_name = source.get('view_name')
                if not view_name:
                     # Fallback sanitization if not provided
                     view_name = source['table_name'].replace(".", "_").replace("/", "_").replace("-", "_")
                
                df.createOrReplaceTempView(view_name)
                
                # Execute Query via Spark SQL
                print(f"DEBUG: Running Spark SQL on view {view_name}: {query}")
                result_df = spark.sql(query)
                
                # Convert to Pandas for return format
                pdf = result_df.toPandas()
                return pdf.astype(object).where(pd.notnull(pdf), None).to_dict('records')
                
            except Exception as e:
                print(f"Error executing Spark SQL: {e}")
                raise e
        
        else:
             raise ValueError("Invalid source provided to execute_sql_query")

    @staticmethod
    async def execute_pipeline(pipeline_id: int, db_session):
        """
        Execute an ETL pipeline with execution history tracking.
        """
        from app.models.etl import ETLPipeline, ETLDataSource, ETLExecution
        from sqlalchemy import select
        from sqlalchemy.orm import joinedload
        import networkx as nx
        import traceback
        from datetime import datetime
        
        # 0. Create Execution Record
        execution = ETLExecution(
            pipeline_id=pipeline_id,
            status="running"
        )
        db_session.add(execution)
        await db_session.commit()
        await db_session.refresh(execution)
        
        try:
            # 1. Fetch pipeline
            result = await db_session.execute(
                select(ETLPipeline).where(ETLPipeline.id == pipeline_id)
            )
            pipeline = result.scalar_one_or_none()
            if not pipeline:
                raise ValueError(f"Pipeline {pipeline_id} not found")
                
            # 2. Build DAG using NetworkX
            G = nx.DiGraph()
            
            # Access nodes and edges directly from JSON columns
            nodes = pipeline.nodes or []
            edges = pipeline.edges or []
            
            if not nodes:
                 raise ValueError("Pipeline has no nodes")

            for node in nodes:
                G.add_node(node['id'], **node)
            for edge in edges:
                G.add_edge(edge['source'], edge['target'])
                
            try:
                execution_order = list(nx.topological_sort(G))
            except nx.NetworkXUnfeasible:
                raise ValueError("Pipeline has cycles!")
                
            # 3. Execute Nodes Recursively
            spark = ETLService.get_spark_session()
            await ETLService._execute_graph_nodes(G, execution_order, spark, db_session, initial_results=None, pipeline_id=pipeline_id)
                        
            # Mark Success
            execution.status = "completed"
            execution.finished_at = datetime.utcnow()
            await db_session.commit()
            
            return {
                "execution_id": execution.id,
                "status": "completed",
                "message": "Pipeline executed successfully"
            }
            
        except Exception as e:
            # Mark Failure
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"Pipeline execution failed: {error_msg}")
            
            execution.status = "failed"
            execution.error_message = error_msg
            execution.finished_at = datetime.utcnow()
            await db_session.commit()
            
            raise e

    @staticmethod
    async def _execute_graph_nodes(G, execution_order, spark, db_session, initial_results=None, pipeline_id: int = None):
        """
        Execute a set of nodes in a graph.
        initial_results: dict mapping node_id -> DataFrame (for injecting inputs into child pipelines)
        Returns a dict of node_id -> DataFrame
        """
        from sqlalchemy import select
        from sqlalchemy.orm import joinedload
        from app.models.etl import ETLDataSource, ETLPipeline
        import networkx as nx
        
        node_results = initial_results.copy() if initial_results else {}
        
        for node_id in execution_order:
            if node_id in node_results:
                # Already computed/injected
                continue
                
            node = G.nodes[node_id]
            node_type = node.get('type')
            node_data = node.get('data', {})
            
            print(f"Executing node {node_id} ({node_type})...")
            
            if node_type == 'source':
                datasource_id = node_data.get('datasourceId')
                selected_columns = node_data.get('selectedColumns')
                
                # Fetch datasource
                ds_result = await db_session.execute(
                    select(ETLDataSource).options(joinedload(ETLDataSource.linked_service)).where(ETLDataSource.id == datasource_id)
                )
                datasource = ds_result.scalar_one_or_none()
                if not datasource:
                    raise ValueError(f"Datasource {datasource_id} not found")
                    
                df = ETLService.load_source_data(datasource, selected_columns)
                node_results[node_id] = df
                
            elif node_type == 'transform':
                generated_code = node_data.get('generatedCode')
                
                # Find upstream input DataFrame
                upstream_nodes = list(G.predecessors(node_id))
                if not upstream_nodes:
                    raise ValueError(f"Transform node {node_id} has no input")
                
                # Prepare inputs for the transform function
                input_dfs = {}
                for uid in upstream_nodes:
                    u_node = G.nodes[uid]
                    # Use table name if available (Source nodes), otherwise fall back to label
                    key = u_node['data'].get('tableName', u_node['data'].get('label', f"node_{uid}"))
                    input_dfs[key] = node_results[uid]
                
                if not generated_code:
                        raise ValueError(f"Transform node {node_id} has no generated code")
                
                # Check if there was schema change in the source tables
                generated_code = await ETLService.check_schema_changes(
                    node_id,
                    node_data,
                    input_dfs,
                    generated_code,
                    db_session,
                    pipeline_id
                )

                # Execute transformation
                import pyspark.sql.functions as F
                import pyspark.sql.types as T
                
                exec_globals = globals().copy()
                exec_globals['F'] = F
                exec_globals['T'] = T
                
                local_vars = {}
                exec(generated_code, exec_globals, local_vars)
                transform_func = local_vars.get('transform')
                
                if not transform_func:
                    raise ValueError(f"No 'transform' function found in generated code for node {node_id}")
                
                result_df = transform_func(spark, input_dfs)
                node_results[node_id] = result_df
                
            elif node_type == 'sink':
                datasource_id = node_data.get('datasourceId')
                table_name = node_data.get('tableName')
                write_mode = node_data.get('writeMode', 'append')
                
                # Find upstream input
                upstream_nodes = list(G.predecessors(node_id))
                if not upstream_nodes:
                    raise ValueError(f"Sink node {node_id} has no input")
                    
                input_df = node_results[upstream_nodes[0]]
                
                # Fetch datasource
                ds_result = await db_session.execute(
                    select(ETLDataSource).options(joinedload(ETLDataSource.linked_service)).where(ETLDataSource.id == datasource_id)
                )
                datasource = ds_result.scalar_one_or_none()
                if not datasource:
                        raise ValueError(f"Sink Datasource {datasource_id} not found")
                
                # Write Data
                ETLService.write_sink_data(input_df, datasource, table_name, write_mode)
                
                # Sink returns input df for continuation if needed
                node_results[node_id] = input_df
            
            elif node_type == 'pipeline':
                child_pipeline_id = node_data.get('pipelineId')
                if not child_pipeline_id:
                    raise ValueError(f"Pipeline node {node_id} not configured")
                    
                # Find upstream input (from parent pipeline)
                upstream_nodes = list(G.predecessors(node_id))
                if not upstream_nodes:
                    raise ValueError(f"Pipeline node {node_id} has no input")
                
                # Data to inject into the child pipeline
                input_df = node_results[upstream_nodes[0]]
                
                # Load Child Pipeline
                result = await db_session.execute(
                    select(ETLPipeline).where(ETLPipeline.id == child_pipeline_id)
                )
                child_pipeline = result.scalar_one_or_none()
                if not child_pipeline:
                    raise ValueError(f"Child Pipeline {child_pipeline_id} not found")
                
                # Build Child Graph
                ChildG = nx.DiGraph()
                c_nodes = child_pipeline.nodes or []
                c_edges = child_pipeline.edges or []
                
                for cn in c_nodes:
                    ChildG.add_node(cn['id'], **cn)
                for ce in c_edges:
                    ChildG.add_edge(ce['source'], ce['target'])
                
                execution_order_child = list(nx.topological_sort(ChildG))
                
                # Identify Child Source Node (Injection Point)
                # Assumption: Child pipeline has exactly one Source Node that we override.
                # Or we look for the Source Node with NO predecessors in the child graph?
                child_source_nodes = [n for n in execution_order_child if ChildG.nodes[n]['type'] == 'source']
                
                if not child_source_nodes:
                     raise ValueError(f"Child pipeline {child_pipeline_id} has no source node to inject data into")
                
                # Inject data into the first source node found
                target_source_id = child_source_nodes[0]
                print(f"DEBUG: Injecting parent data into child node {target_source_id}")
                
                child_results = {
                    target_source_id: input_df
                }
                
                # Recursively Execute Child Graph
                final_child_results = await ETLService._execute_graph_nodes(
                    ChildG, execution_order_child, spark, db_session, initial_results=child_results
                )
                
                # Determine Output of Child Pipeline
                # We return the result of the LAST executed node in the child pipeline?
                # Or find the node with NO successors (Leaf)?
                leaf_nodes = [n for n in ChildG.nodes() if ChildG.out_degree(n) == 0]
                
                if not leaf_nodes:
                    # Should be impossible if DAG is valid and not empty
                     node_results[node_id] = input_df 
                else:
                    # Return result of the first leaf node. 
                    # Prefer Sink or Transform result.
                    # Note: If child pipeline ends in sink, we return data flowing TO sink (from write_sink_data logic above)
                    output_node_id = leaf_nodes[0]
                    node_results[node_id] = final_child_results[output_node_id]

        return node_results
            
    @staticmethod
    async def generate_transformation_code(prompt: str, upstream_schemas: dict, model_name: str = "gpt-4o", api_key: str = None) -> str:
        """
        Generate PySpark transformation code using LLM.
        Supports dynamic model selection without heavyweight dependencies.
        """
        from app.services.llm_models import LLMModelFactory
        from langchain_core.messages import SystemMessage, HumanMessage

        # Dynamically select model provider based on model_name
        factory = LLMModelFactory()
        llm = factory.create_llm(
            model_name=model_name,
            temperature=0.1,
            max_tokens=2000, # Default higher token limit for code generation
            api_key=api_key
        )
        
        # Format schema description for multiple tables
        schema_lines = []
        for table_name, schema in upstream_schemas.items():
            schema_lines.append(f"Table '{table_name}':")
            for col, type_ in schema.items():
                schema_lines.append(f"  - {col}: {type_}")
        schema_desc = "\n".join(schema_lines)
        
        system_prompt = """You are an expert PySpark Data Engineer.
Your task is to write a single Python function named `transform` that performs the requested data transformation.
The function signature MUST be:
def transform(spark, input_dfs):

`input_dfs` is a dictionary where keys are table names and values are Spark DataFrames.
You can access your input tables like: `df = input_dfs['MyTable']`

It must return a single DataFrame.

1. Return ONLY the python function code. No markdown formatting, no explanations.
2. Assume standard imports (pyspark.sql.functions as F, types as T) are available.
3. Do not create a SparkSession, use the one provided.
"""
        
        user_message = f"""
Available Input Schemas:
{schema_desc}

User Request: {prompt}

Write the 'transform' function.
"""
        
        # Invoke LLM
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message)
        ]
        
        response = await llm.ainvoke(messages)
        content = response.content
        
        # Clean markdown if present
        code = content.replace("```python", "").replace("```", "").strip()
        
        return code

    @staticmethod
    async def check_schema_changes(
        node_id: str,
        node_data: Dict[str, Any],
        input_dfs: Dict[str, Any],
        current_code: str,
        db_session=None,
        pipeline_id: int = None
    ) -> str:
        """
        Validates schema and triggers auto-heal if needed. 
        Returns valid code (original or fixed).
        """
        from app.models.etl import ETLPipeline
        import json

        saved_schema = node_data.get('sourceSchema')
        if not saved_schema:
            return current_code

        live_schema = {}
        for t_name, df_in in input_dfs.items():
            live_schema[t_name] = {f.name: str(f.dataType) for f in df_in.schema.fields}
        
        if json.dumps(saved_schema, sort_keys=True) == json.dumps(live_schema, sort_keys=True):
            return current_code

        print(f"DEBUG: Schema mismatch detected for node {node_id}. Attempting Auto-Heal...")
        try:
            new_code = await ETLService.fix_transformation_code(
                current_code,
                saved_schema,
                live_schema
            )
            print(f"DEBUG: Auto-Heal successful.")
            
            # Persist if possible
            if db_session and pipeline_id:
                try:
                    pipeline = await db_session.get(ETLPipeline, pipeline_id)
                    if pipeline and pipeline.nodes:
                        updated_nodes = list(pipeline.nodes)
                        for idx, n in enumerate(updated_nodes):
                            if n['id'] == node_id:
                                n['data']['generatedCode'] = new_code
                                n['data']['sourceSchema'] = live_schema
                                updated_nodes[idx] = n
                                break
                        
                        pipeline.nodes = updated_nodes
                        from sqlalchemy.orm.attributes import flag_modified
                        flag_modified(pipeline, "nodes")
                        await db_session.commit()
                        print(f"DEBUG: Persisted auto-healed code to DB.")
                except Exception as db_err:
                    print(f"WARNING: Failed to persist auto-heal to DB: {db_err}")
            
            return new_code

        except Exception as heal_err:
            print(f"WARNING: Auto-Heal failed: {heal_err}. Proceeding with original code.")
            return current_code

    @staticmethod
    async def fix_transformation_code(
        current_code: str,
        old_schema: Dict[str, Dict[str, str]],
        new_schema: Dict[str, Dict[str, str]],
        model_name: str = "gpt-4o",
        api_key: str = None
    ) -> str:
        """
        Refactor PySpark code to adapt to schema changes using LLM.
        """
        from app.services.llm_models import LLMModelFactory
        from langchain_core.messages import SystemMessage, HumanMessage

        factory = LLMModelFactory()
        llm = factory.create_llm(
            model_name=model_name,
            temperature=0.1,
            max_tokens=2000,
            api_key=api_key
        )

        # Format schemas
        def format_schema(schemas):
            lines = []
            for table, schema in schemas.items():
                lines.append(f"Table '{table}':")
                for col, type_ in schema.items():
                    lines.append(f"  - {col}: {type_}")
            return "\n".join(lines)

        old_desc = format_schema(old_schema)
        new_desc = format_schema(new_schema)

        system_prompt = """You are an expert PySpark Data Engineer.
Your task is to REFACTOR an existing PySpark transformation function to work with a CHANGED schema.

The goal is to preserve the original logic/intent while fixing column references or types that have changed.

The function signature MUST remain:
def transform(spark, input_dfs):

1. Return ONLY the python function code. No markdown formatting, no explanations.
2. Assume standard imports (pyspark.sql.functions as F, types as T) are available.
3. If a column was renamed, update the reference. If a column was deleted, handle it gracefully or remove the logic if impossible.
"""

        user_message = f"""
Existing Code:
{current_code}

OLD Schema:
{old_desc}

NEW Schema:
{new_desc}

Please fix the code to match the NEW Schema.
"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_message)
        ]

        response = await llm.ainvoke(messages)
        content = response.content
        
        # Clean markdown
        code = content.replace("```python", "").replace("```", "").strip()
        return code
