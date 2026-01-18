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
    def load_source_data(datasource, selected_columns, limit=None):
        """
        Load data from a source datasource with selected columns.
        Returns a Spark DataFrame.
        """
        from pyspark.sql import SparkSession
        from app.core.security import decrypt_value
        import tempfile
        import json
        
        spark = ETLService.get_spark_session()
        
        # Access Linked Service
        # Ensure linked_service is loaded (caller responsibility)
        ls = datasource.linked_service
        if not ls:
             raise ValueError("Linked Service not found for data source")
             
        db_type = ls.service_type
        config = ls.connection_config.copy()
        
        # Decrypt sensitive fields
        sensitive_keys = ["password", "secret_key", "account_key", "access_key", "credentials_json"]
        for key in sensitive_keys:
            if key in config:
                config[key] = decrypt_value(config[key])
            config['credentials_json'] = decrypt_value(config['credentials_json'])
        
        # JDBC Sources
        if db_type in ['postgresql', 'mysql', 'sql_server', 'azure_sql']:
            table_name = datasource.table_name
            connection_string = ETLService._build_connection_string(db_type, config)
            
            # Parse connection string to get JDBC URL
            if db_type == 'postgresql':
                jdbc_url = connection_string.replace('postgresql://', 'jdbc:postgresql://')
            elif db_type == 'mysql':
                jdbc_url = connection_string.replace('mysql+pymysql://', 'jdbc:mysql://')
            elif db_type in ['sql_server', 'azure_sql']:
                jdbc_url = connection_string.replace('mssql+pyodbc://', 'jdbc:sqlserver://')
            
            # Load data
            df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", config.get('username', '')) \
                .option("password", config.get('password', '')) \
                .load()
        
        elif db_type == 'bigquery':
            project_id = config.get('project_id')
            dataset_id = config.get('dataset_id')
            table_id = datasource.table_name
            
            # Configure BigQuery read with Base64 credentials
            # Requires spark-bigquery-connector 0.25+ or so
            import base64
            
            creds_json_str = config['credentials_json']
            if isinstance(creds_json_str, str):
                # Ensure it is a valid JSON string
                pass 
            
            # Base64 encode the JSON
            creds_b64 = base64.b64encode(creds_json_str.encode('utf-8')).decode('utf-8')
            
            full_table_id = f"{dataset_id}.{table_id}"
            if project_id:
                full_table_id = f"{project_id}.{full_table_id}"

            reader = spark.read \
                .format("bigquery") \
                .option("viewsEnabled", "true") \
                .option("materializationDataset", dataset_id) \
                .option("credentials", creds_b64)
            
            if project_id:
                 # Set parent project for billing
                 reader = reader.option("parentProject", project_id)

            df = reader.load(full_table_id)
            
        elif db_type in ['s3', 'minio', 'gcs', 'adls']:
            # Configure FileSystem
            sc = spark.sparkContext
            conf = sc._jsc.hadoopConfiguration()
            
            path = datasource.table_name
            # Determine format based on extension (simple heuristic)
            fmt = "parquet"
            if path and path.endswith(".csv"): fmt = "csv"
            elif path and path.endswith(".json"): fmt = "json"
            elif path and path.endswith(".txt"): fmt = "text"
            
            if db_type in ['s3', 'minio']:
                access_key = config.get('access_key')
                secret_key = config.get('secret_key')
                endpoint = config.get('endpoint')
                
                if access_key: conf.set("fs.s3a.access.key", access_key)
                if secret_key: conf.set("fs.s3a.secret.key", secret_key)
                if endpoint:
                    conf.set("fs.s3a.endpoint", endpoint)
                    conf.set("fs.s3a.path.style.access", "true")
                
                # Normalize path
                if not path.startswith("s3a://"):
                    bucket = config.get('bucket')
                    if bucket and not path.startswith(bucket):
                         path = f"s3a://{bucket}/{path.lstrip('/')}"
                    else:
                         path = f"s3a://{path}"

            elif db_type == 'gcs':
                # GCS Creds
                if 'credentials_json' in config:
                     import json
                     creds = json.loads(config['credentials_json'])
                     conf.set("fs.gs.auth.service.account.email", creds.get('client_email'))
                     conf.set("fs.gs.auth.service.account.private.key", creds.get('private_key'))
                     conf.set("fs.gs.auth.service.account.private.key.id", creds.get('private_key_id'))
                     conf.set("google.cloud.auth.service.account.enable", "true")
                     # Unset keyfile to avoid conflict
                     conf.unset("fs.gs.auth.service.account.json.keyfile")
                
                if not path.startswith("gs://"):
                    bucket = config.get('bucket')
                    if bucket and not path.startswith(bucket):
                         path = f"gs://{bucket}/{path.lstrip('/')}"
                    else:
                          path = f"gs://{path}"
                          
            elif db_type == 'adls':
                # Azure Data Lake Storage Gen2 (abfss://)
                account_name = config.get('account_name')
                account_key = config.get('account_key')
                
                if account_name and account_key:
                    conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)
                
                if not path.startswith("abfss://"):
                    container = config.get('container') # 'bucket' equivalent
                    if container and not path.startswith(container):
                         path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{path.lstrip('/')}"
                    else:
                         # Assume user provided full path or partial
                         path = f"abfss://{path}@{account_name}.dfs.core.windows.net" if not "dfs.core.windows.net" in path else path
                         
            reader = spark.read.format(fmt)
            if fmt == "csv": reader = reader.option("header", "true").option("inferSchema", "true")
            df = reader.load(path)

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        # Common post-processing
        if selected_columns:
            # Verify columns exist (case sensitivity might be an issue)
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
        from app.core.security import decrypt_value
        import tempfile
        import json
        
        try:
            spark = ETLService.get_spark_session()
            config = connection_config.copy()
            
            # Decrypt if keys look encrypted (starts with gAAAA...)
            def is_encrypted(s):
                return isinstance(s, str) and s.startswith('gAAAA')

            if 'password' in config and is_encrypted(config['password']):
                 config['password'] = decrypt_value(config['password'])
            if 'credentials_json' in config and is_encrypted(config['credentials_json']):
                 config['credentials_json'] = decrypt_value(config['credentials_json'])
            if 'secret_key' in config and is_encrypted(config['secret_key']):
                 config['secret_key'] = decrypt_value(config['secret_key'])
            if 'access_key' in config and is_encrypted(config['access_key']):
                 config['access_key'] = decrypt_value(config['access_key'])
            if 'account_key' in config and is_encrypted(config['account_key']):
                 config['account_key'] = decrypt_value(config['account_key'])

            if db_type in ['postgresql', 'mysql', 'sql_server', 'azure_sql']:
                # Build JDBC URL
                if db_type == 'postgresql':
                    url = f"jdbc:postgresql://{config.get('host', 'localhost')}:{config.get('port', 5432)}/{config.get('database', '')}"
                elif db_type == 'mysql':
                    url = f"jdbc:mysql://{config.get('host', 'localhost')}:{config.get('port', 3306)}/{config.get('database', '')}"
                elif db_type in ['sql_server', 'azure_sql']:
                    url = f"jdbc:sqlserver://{config.get('server')};databaseName={config.get('database')}"
                
                # Use dummy query if no table specified
                target_table = table_name if table_name else "(SELECT 1) as test_connection"
                
                df = spark.read \
                    .format("jdbc") \
                    .option("url", url) \
                    .option("dbtable", target_table) \
                    .option("user", config.get('username', '')) \
                    .option("password", config.get('password', '')) \
                    .load()
                    
            elif db_type in ['s3', 'minio', 'gcs']:
                # Test Bucket Access
                sc = spark.sparkContext
                conf = sc._jsc.hadoopConfiguration()
                
                test_path = table_name if table_name else ""
                
                if db_type in ['s3', 'minio']:
                    if 'access_key' in config: conf.set("fs.s3a.access.key", config['access_key'])
                    if 'secret_key' in config: conf.set("fs.s3a.secret.key", config['secret_key'])
                    if 'endpoint' in config:
                         conf.set("fs.s3a.endpoint", config['endpoint'])
                         conf.set("fs.s3a.path.style.access", "true")
                         
                    if not test_path:
                        bucket = config.get('bucket')
                        test_path = f"s3a://{bucket}/" if bucket else "s3a:///"
                    elif not test_path.startswith("s3a://"):
                        test_path = f"s3a://{test_path}"

                elif db_type == 'gcs':
                    if 'credentials_json' in config:
                         import json
                         creds = json.loads(config['credentials_json'])
                         conf.set("fs.gs.auth.service.account.email", creds.get('client_email'))
                         conf.set("fs.gs.auth.service.account.private.key", creds.get('private_key'))
                         conf.unset("fs.gs.auth.service.account.json.keyfile")
                         conf.set("fs.gs.auth.service.account.private.key.id", creds.get('private_key_id'))
                         conf.set("google.cloud.auth.service.account.enable", "true")
                    
                    if not test_path:
                        bucket = config.get('bucket')
                        test_path = f"gs://{bucket}/" if bucket else "gs:///"
                    elif not test_path.startswith("gs://"):
                        test_path = f"gs://{test_path}"

                elif db_type == 'adls':
                    account_name = config.get('account_name')
                    account_key = config.get('account_key')
                    if account_name and account_key:
                        conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)
                    
                    if not test_path:
                        container = config.get('container')
                        test_path = f"abfss://{container}@{account_name}.dfs.core.windows.net/" if container else f"abfss://@{account_name}.dfs.core.windows.net/"
                    elif "dfs.core.windows.net" not in test_path:
                         container = config.get('container')
                         test_path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{test_path}"

                # Validate connection
                if table_name:
                     # If specific file/folder provided, try to read schema/first row
                     fmt = "parquet"
                     if table_name.endswith(".csv"): fmt = "csv"
                     elif table_name.endswith(".json"): fmt = "json"
                     elif table_name.endswith(".txt"): fmt = "text"
                     
                     spark.read.format(fmt).load(test_path).limit(1).collect()
                else:
                     # If only bucket/container provided, check access by listing status
                     # This ensures credentials are valid even without reading a specific file
                     try:
                         Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
                         fs = Path(test_path).getFileSystem(conf)
                         fs.listStatus(Path(test_path))
                     except Exception as e:
                         # Enhance error message
                         raise Exception(f"Failed to access bucket/container: {str(e)}")
                
            elif db_type == 'bigquery':
                project_id = config.get('project_id')
                dataset_id = config.get('dataset_id')
                
                df.limit(1).collect()
                
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
            # We can reuse load_source_data with a small limit to infer schema
            df = ETLService.load_source_data(datasource, selected_columns=None, limit=1)
            
            # Extract detailed schema
            return [
                {
                    'name': field.name,
                    'type': str(field.dataType).replace('()', ''), # Clean up type string
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
        from app.core.security import decrypt_value
        import tempfile
        import json
        
        ls = datasource.linked_service
        if not ls:
             # If lazy load failed or not present (should be eager loaded before)
             raise ValueError("Linked Service not found for sink datasource")
        db_type = ls.service_type
        config = ls.connection_config.copy()
        
        # Decrypt sensitive fields
        if 'password' in config:
            config['password'] = decrypt_value(config['password'])
        if 'credentials_json' in config:
            config['credentials_json'] = decrypt_value(config['credentials_json'])
        if 'secret_key' in config:
             config['secret_key'] = decrypt_value(config['secret_key'])
        if 'access_key' in config:
             config['access_key'] = decrypt_value(config['access_key'])
        if 'account_key' in config:
             config['account_key'] = decrypt_value(config['account_key'])
            
        if db_type in ['postgresql', 'mysql', 'sql_server', 'azure_sql']:
            connection_string = ETLService._build_connection_string(db_type, config)
            # Parse connection string to get JDBC URL (reusing logic from load_source_data)
            if db_type == 'postgresql':
                jdbc_url = connection_string.replace('postgresql://', 'jdbc:postgresql://')
            elif db_type == 'mysql':
                jdbc_url = connection_string.replace('mysql+pymysql://', 'jdbc:mysql://')
            elif db_type in ['sql_server', 'azure_sql']:
                jdbc_url = connection_string.replace('mssql+pyodbc://', 'jdbc:sqlserver://')
            
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
            
        elif db_type == 'bigquery':
            project_id = config.get('project_id')
            dataset_id = config.get('dataset_id')
            
            # Credentials handling (Base64)
            import base64
            creds_json_str = config['credentials_json']
            creds_b64 = base64.b64encode(creds_json_str.encode('utf-8')).decode('utf-8')
            
            # Configure Spark Context Hadoop Configuration for GCS independently
            try:
                sc = df.sparkSession.sparkContext
                hconf = sc._jsc.hadoopConfiguration()
                
                # Parse JSON for GCS auth (BigQuery write often uses GCS under hood)
                import json
                creds = json.loads(creds_json_str)
                hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                hconf.set("google.cloud.auth.service.account.enable", "true")
                hconf.set("fs.gs.auth.service.account.email", creds.get('client_email'))
                hconf.set("fs.gs.auth.service.account.private.key", creds.get('private_key'))
                hconf.set("fs.gs.auth.service.account.private.key.id", creds.get('private_key_id'))
                
            except Exception as e:
                print(f"Warning: Failed to set Hadoop configuration: {e}")

            full_table_id = f"{dataset_id}.{table_name}"
            if project_id:
                full_table_id = f"{project_id}.{full_table_id}"
            
            # Use Direct Write method (Storage Write API)
            writer = df.write \
                .format("bigquery") \
                .option("credentials", creds_b64) \
                .option("writeMethod", "direct")
            
            if project_id:
                writer = writer.option("parentProject", project_id)
            
            # Save mode
            writer.mode(mode).save(full_table_id)
            
            # Save mode
            writer.mode(mode).save(full_table_id)

        elif db_type in ['s3', 'minio', 'gcs', 'adls']:
            # Configure FileSystem
            sc = df.sparkSession.sparkContext
            conf = sc._jsc.hadoopConfiguration()
            
            path = table_name
            # Determine format
            fmt = "parquet"
            if path and path.endswith(".csv"): fmt = "csv"
            elif path and path.endswith(".json"): fmt = "json"
            elif path and path.endswith(".txt"): fmt = "text"
            
            if db_type in ['s3', 'minio']:
                access_key = config.get('access_key')
                secret_key = config.get('secret_key')
                endpoint = config.get('endpoint')
                
                if access_key: conf.set("fs.s3a.access.key", access_key)
                if secret_key: conf.set("fs.s3a.secret.key", secret_key)
                if endpoint:
                    conf.set("fs.s3a.endpoint", endpoint)
                    conf.set("fs.s3a.path.style.access", "true")
                
                # Normalize path
                if not path.startswith("s3a://"):
                    bucket = config.get('bucket')
                    if bucket and not path.startswith(bucket):
                         path = f"s3a://{bucket}/{path.lstrip('/')}"
                    else:
                         path = f"s3a://{path}"

            elif db_type == 'gcs':
                # GCS Creds
                if 'credentials_json' in config:
                     import json
                     creds = json.loads(config['credentials_json'])
                     conf.set("fs.gs.auth.service.account.email", creds.get('client_email'))
                     conf.set("fs.gs.auth.service.account.private.key", creds.get('private_key'))
                     conf.set("fs.gs.auth.service.account.private.key.id", creds.get('private_key_id'))
                     conf.set("google.cloud.auth.service.account.enable", "true")
                     # Unset keyfile to avoid conflict
                     conf.unset("fs.gs.auth.service.account.json.keyfile")
                
                if not path.startswith("gs://"):
                    bucket = config.get('bucket')
                    if bucket and not path.startswith(bucket):
                         path = f"gs://{bucket}/{path.lstrip('/')}"
                    else:
                         path = f"gs://{path}"

            elif db_type == 'adls':
                account_name = config.get('account_name')
                account_key = config.get('account_key')
                
                if account_name and account_key:
                    conf.set(f"fs.azure.account.key.{account_name}.dfs.core.windows.net", account_key)
                
                if not path.startswith("abfss://"):
                    container = config.get('container') 
                    if container and not path.startswith(container):
                         path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{path.lstrip('/')}"
                    else:
                         path = f"abfss://{path}@{account_name}.dfs.core.windows.net" if not "dfs.core.windows.net" in path else path
            
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
        config = ls.connection_config.copy()

        # Decrypt sensitive fields
        if 'password' in config:
            config['password'] = decrypt_value(config['password'])
        if 'credentials_json' in config:
            config['credentials_json'] = decrypt_value(config['credentials_json'])

        url = ""
        connect_args = {}

        if db_type == 'postgresql':
            url = f"postgresql://{config.get('username')}:{config.get('password')}@{config.get('host')}:{config.get('port')}/{config.get('database')}"
        elif db_type == 'mysql':
            url = f"mysql+pymysql://{config.get('username')}:{config.get('password')}@{config.get('host')}:{config.get('port')}/{config.get('database')}"
        elif db_type == 'sql_server':
            url = f"mssql+pymysql://{config.get('username')}:{config.get('password')}@{config.get('host')}:{config.get('port')}/{config.get('database')}"
        elif db_type == 'bigquery':
            url = f"bigquery://{config.get('project_id')}"
            # Pass credentials info as dict directly to engine
            if 'credentials_json' in config:
                try:
                    import json
                    creds_dict = json.loads(config['credentials_json'])
                    # 'credentials_info' is supported by pybigquery
                    connect_args = {'credentials_info': creds_dict}
                except Exception as e:
                    print(f"Error parsing BQ credentials: {e}")
        else:
            raise ValueError(f"Unsupported database type for SQL Agent: {db_type}")

        return create_engine(url, connect_args=connect_args)

    @staticmethod
    async def execute_sql_query(engine, query: str) -> List[Dict[str, Any]]:
        """
        Execute raw SQL query using provided SQLAlchemy engine and return results as list of dicts.
        """
        import pandas as pd
        from sqlalchemy import text
        from typing import List, Dict, Any
        
        # Helper to run synchronous SQL code in async wrapper if needed, 
        # or just use pandas which is sync. For MVP, sync pandas is fine if not blocking main loop too much, 
        # or wrap in run_in_executor.
        
        try:
            # We use pandas for easy DF conversion
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                
            # Convert to list of dicts (JSON serializable)
            # Handle timestamps etc? Pandas to_dict('records') handles basics.
            # Timestamp to string might be needed.
            return df.astype(object).where(pd.notnull(df), None).to_dict('records')
        except Exception as e:
            print(f"Error executing SQL: {e}")
            raise e

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
