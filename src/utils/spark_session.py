"""
Spark session factory with Windows environment auto-configuration.

Handles three Windows-specific issues:
  1. JAVA_HOME pointing to wrong JDK version
  2. Missing winutils.exe / hadoop.dll for Parquet/Delta writes
  3. Windows Store python alias hijacking Spark worker Python path
"""

import os
import sys
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _setup_windows_env():
    """Fix Windows-specific PySpark environment issues."""
    if os.name != "nt":
        return

    # 1. Fix JAVA_HOME — find the actual JDK installation
    java_base = Path(r"C:\Program Files\Java")
    if java_base.exists():
        jdk_dirs = sorted(java_base.glob("jdk-*"), reverse=True)
        if jdk_dirs:
            java_home = str(jdk_dirs[0])
            os.environ["JAVA_HOME"] = java_home
            print(f"[ENV] JAVA_HOME = {java_home}")

    # 2. Fix HADOOP_HOME — look for winutils.exe
    # Check project-local .hadoop first, then the scratch project's copy
    candidates = [
        PROJECT_ROOT / ".hadoop",
        Path(r"C:\Users\bharg\.gemini\antigravity\scratch\paysim-fraud-platform\.hadoop"),
    ]
    for hadoop_dir in candidates:
        winutils = hadoop_dir / "bin" / "winutils.exe"
        if winutils.exists():
            os.environ["HADOOP_HOME"] = str(hadoop_dir)
            # Add to PATH so hadoop.dll is found
            bin_dir = str(hadoop_dir / "bin")
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
            print(f"[ENV] HADOOP_HOME = {hadoop_dir}")
            break
    else:
        # Download winutils if not found anywhere
        _download_winutils()

    # 3. Fix PYSPARK_PYTHON — prevent Windows Store alias hijack
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def _download_winutils():
    """Download winutils.exe and hadoop.dll for Hadoop 3.3.0 on Windows."""
    import urllib.request

    hadoop_dir = PROJECT_ROOT / ".hadoop" / "bin"
    hadoop_dir.mkdir(parents=True, exist_ok=True)

    base_url = "https://github.com/kontext-tech/winutils/raw/master/hadoop-3.3.0/bin"
    for fname in ("winutils.exe", "hadoop.dll"):
        target = hadoop_dir / fname
        if not target.exists():
            print(f"[ENV] Downloading {fname}...")
            urllib.request.urlretrieve(f"{base_url}/{fname}", str(target))

    os.environ["HADOOP_HOME"] = str(hadoop_dir.parent)
    bin_dir = str(hadoop_dir)
    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
    print(f"[ENV] HADOOP_HOME = {hadoop_dir.parent}")


def get_spark(app_name="EFIP"):
    """Create a production SparkSession with Delta Lake support."""
    _setup_windows_env()

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[SPARK] Session created: {app_name}")
    return spark


def get_spark_for_test():
    """Lightweight Spark session for unit tests."""
    _setup_windows_env()

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("EFIP-Test")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
