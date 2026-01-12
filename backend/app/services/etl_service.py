import os
import sys
import requests
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

            # Ensure correct python executable is used for workers and driver
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
            # Note: In production, we might submit jobs to a cluster.
            # Here we run local mode.
            builder = SparkSession.builder \
                .appName("ChatbotETL") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .config("spark.jars", driver_path) \
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            
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
        if 'password' in config:
            config['password'] = decrypt_value(config['password'])
        if 'credentials_json' in config:
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
            
            # Write credentials to temp file
            # Note: In a real persistent scenario we might manage key files differently.
            # Using tempfile here is safe enough for preview.
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(json.loads(config['credentials_json']), f)
                credentials_path = f.name
            
            # Configure BigQuery read
            # Requires spark-bigquery-with-dependencies jar loaded in session
            full_table_id = f"{dataset_id}.{table_id}"
            if project_id:
                full_table_id = f"{project_id}.{full_table_id}"

            # Use direct table read instead of query to avoid materialization dataset issues.
            # Spark BQ connector pushes down filters and limits efficiently.
            reader = spark.read \
                .format("bigquery") \
                .option("viewsEnabled", "true") \
                .option("materializationDataset", dataset_id) \
                .option("credentialsFile", credentials_path)
            
            if project_id:
                 # Set parent project for billing
                 reader = reader.option("parentProject", project_id)

            df = reader.load(full_table_id)
            
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
            if 'credentials_json' in config:
                 if is_encrypted(config['credentials_json']):
                     config['credentials_json'] = decrypt_value(config['credentials_json'])

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
                
                # Try to load one row to verify permissions
                df.limit(1).collect()
                
            elif db_type == 'bigquery':
                project_id = config.get('project_id')
                dataset_id = config.get('dataset_id')
                
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(json.loads(config['credentials_json']), f)
                    credentials_path = f.name
                
                reader = spark.read \
                    .format("bigquery") \
                    .option("viewsEnabled", "true") \
                    .option("materializationDataset", dataset_id) \
                    .option("credentialsFile", credentials_path)
                
                if project_id:
                    reader = reader.option("parentProject", project_id)
                
                if table_name:
                    full_table_id = f"{dataset_id}.{table_name}"
                    if project_id:
                        full_table_id = f"{project_id}.{full_table_id}"
                    df = reader.load(full_table_id)
                else:
                    # Test connection with a simple query
                    df = reader.option("query", "SELECT 1").load()
                
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
            
            # Credentials handling
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(json.loads(config['credentials_json']), f)
                credentials_path = f.name
            
            # Configure Spark Context Hadoop Configuration for GCS independently
            # This is required because GCS connector might not pick up bigquery options for internal writes
            try:
                sc = df.sparkSession.sparkContext
                hconf = sc._jsc.hadoopConfiguration()
                hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
                hconf.set("google.cloud.auth.service.account.enable", "true")
                hconf.set("google.cloud.auth.service.account.json.keyfile", credentials_path)
            except Exception as e:
                print(f"Warning: Failed to set Hadoop configuration: {e}")

            full_table_id = f"{dataset_id}.{table_name}"
            if project_id:
                full_table_id = f"{project_id}.{full_table_id}"
            
            # Use Direct Write method (Storage Write API) to avoid GCS dependency
            # This simplifies setup as the user doesn't need a GCS bucket or GCS permissions.
            writer = df.write \
                .format("bigquery") \
                .option("credentialsFile", credentials_path) \
                .option("writeMethod", "direct")
            
            if project_id:
                writer = writer.option("parentProject", project_id)
            
            # Save mode
            writer.mode(mode).save(full_table_id)
            
        else:
            raise ValueError(f"Unsupported sink database type: {db_type}")

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
                
            # 3. Execute Nodes
            # Store DataFrames in a dictionary keyed by Node ID
            node_results = {}
            
            # Initialize Spark
            spark = ETLService.get_spark_session()
            
            for node_id in execution_order:
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
                    # We map table names (from source nodes) to DataFrames to match the AI generation context
                    input_dfs = {}
                    for uid in upstream_nodes:
                        u_node = G.nodes[uid]
                        # Use table name if available (Source nodes), otherwise fall back to label
                        key = u_node['data'].get('tableName', u_node['data'].get('label', f"node_{uid}"))
                        input_dfs[key] = node_results[uid]
                    
                    if not generated_code:
                         raise ValueError(f"Transform node {node_id} has no generated code")
                    
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
                    # For now only Postgres sink supported via JDBC
                    # TODO: Abstract this based on Linked Service type
                    if datasource.linked_service and datasource.linked_service.service_type == 'postgresql':
                        db_config = datasource.linked_service.connection_config
                        jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
                        
                        writer = input_df.write \
                            .format("jdbc") \
                            .option("url", jdbc_url) \
                            .option("dbtable", table_name) \
                            .option("user", db_config['user']) \
                            .option("password", db_config['password']) \
                            .option("driver", "org.postgresql.Driver") \
                            .mode(write_mode)
                            
                        writer.save()
                    else:
                        print(f"Unsupported sink type: {datasource.linked_service.service_type if datasource.linked_service else 'None'}")
                        
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
    async def generate_transformation_code(prompt: str, upstream_schemas: dict, model_name: str = "gpt-4o", api_key: str = None) -> str:
        """
        Generate PySpark transformation code using LLM.
        Supports dynamic model selection without heavyweight dependencies.
        """
        from langchain_openai import ChatOpenAI
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import SystemMessage, HumanMessage
        from app.core.config import settings

        # Dynamically select model provider based on model_name
        model_lower = model_name.lower()

        if any(x in model_lower for x in ['gpt', 'openai']):
            llm = ChatOpenAI(
                model=model_name,
                temperature=0.1,
                api_key=api_key or settings.OPENAI_API_KEY
            )
        elif any(x in model_lower for x in ['claude', 'anthropic']):
            llm = ChatAnthropic(
                model=model_name,
                temperature=0.1,
                api_key=api_key or settings.ANTHROPIC_API_KEY
            )
        else:
            # Fallback to GPT-4o if unknown or default
            llm = ChatOpenAI(
                model="gpt-4o",
                temperature=0.1,
                api_key=api_key or settings.OPENAI_API_KEY
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
