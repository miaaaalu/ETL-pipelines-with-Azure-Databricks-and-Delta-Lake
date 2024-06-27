# Databricks notebook source
# MAGIC %md
# MAGIC # Five Methods to Access Azure Storage Account from Databricks
# MAGIC
# MAGIC ## Recommended Usage
# MAGIC In large enterprise environments for data pipeline management, `Service Principal` or `Credential Passthrough` are recommended due to their higher security and flexibility:
# MAGIC - **Service Principal:** Suitable for high-security production environments with strict access control needs.
# MAGIC - **Credential Passthrough:** Ideal for multi-user environments leveraging existing Azure AD for transparent authentication and minimizing credential management.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Access via Access Key
# MAGIC - **Description**: Using the storage account access key to access data. This is the most basic and direct method.
# MAGIC - **Pros**:
# MAGIC   - Simple and quick setup.
# MAGIC   - Suitable for quick testing and development.
# MAGIC - **Cons**:
# MAGIC   - Lower security since anyone with the key can access the storage account.
# MAGIC   - Difficult to manage and risky for large-scale use.
# MAGIC - **Recommended Scenarios**:
# MAGIC   - Development and testing environments.
# MAGIC   - Temporary or one-time data access.
# MAGIC - **Example**:
# MAGIC
# MAGIC ```py
# MAGIC   
# MAGIC   storage_account = "your_storage_account"
# MAGIC   storage_account_key = "your_storage_account_key"
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.account.key.<storage_account>.blob.core.windows.net".format
# MAGIC       storage_account_key
# MAGIC   )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Access via SAS Token
# MAGIC
# MAGIC - **Description**: Using a Shared Access Signature (SAS) token to restrict access. SAS tokens can set expiration times and permissions scope.
# MAGIC - **Pros**:
# MAGIC   - High flexibility with permissions and expiry.
# MAGIC   - Safer as access is time-limited and scoped.
# MAGIC - **Cons**:
# MAGIC   - Requires periodic generation and management of SAS tokens.
# MAGIC   - Higher management complexity.
# MAGIC - **Recommended Scenarios**:
# MAGIC   - Short-term access with restricted permissions.
# MAGIC   - Temporary data sharing and access.
# MAGIC - **Example**:
# MAGIC
# MAGIC ```py
# MAGIC   
# MAGIC   storage_account = "your_storage_account"
# MAGIC   sas_token_key = "your_sas_token_key"
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.account.auth.type.<storage_account>.dfs.core.windows.net", 
# MAGIC       "SAS"
# MAGIC       )
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.sas.token.provider.type.<storage_account>.dfs.core.windows.net", 
# MAGIC       "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
# MAGIC       )
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.sas.fixed.token.<storage_account>.dfs.core.windows.net", 
# MAGIC       dbutils.secrets.get(<sas_token_key>)
# MAGIC       )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Access via Service Principal
# MAGIC
# MAGIC - **Description**: Using Azure Active Directory (AAD) service principal for authentication and authorization.
# MAGIC - **Pros**:
# MAGIC   - High security through AAD authentication and authorization.
# MAGIC   - Fine-grained access control.
# MAGIC - **Cons**:
# MAGIC   - Requires additional setup for service principal and permissions.
# MAGIC   - More complex configuration and management.
# MAGIC - **Recommended Scenarios**:
# MAGIC   - High-security requirements in production environments.
# MAGIC   - Large-scale enterprise environments needing strict access control.
# MAGIC - **Example**:
# MAGIC
# MAGIC ```py
# MAGIC   storage_account = "your_storage_account"
# MAGIC   application_id = "your_client_id"
# MAGIC   service_credential = "your_client_Secret"
# MAGIC   directory_id = "your_tenant_id"
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.account.auth.type.<storage_account>.dfs.core.windows.net", 
# MAGIC       "OAuth"
# MAGIC       )
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.account.oauth.provider.type.<storage_account>.dfs.core.windows.net", 
# MAGIC       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
# MAGIC       )
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.account.oauth2.client.id.<storage_account>.dfs.core.windows.net", 
# MAGIC       "<application_id>"
# MAGIC       )
# MAGIC
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.account.oauth2.client.secret.<storage_account>.dfs.core.windows.net", 
# MAGIC       <service_credential>
# MAGIC       )
# MAGIC       
# MAGIC   spark.conf.set(
# MAGIC       "fs.azure.account.oauth2.client.endpoint.<storage_account>.dfs.core.windows.net", 
# MAGIC       "https://login.microsoftonline.com/<directory_id>/oauth2/token"
# MAGIC       )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Access via Cluster Scoped Authentication
# MAGIC
# MAGIC - **Description**: Configuring authentication at the cluster level, typically for cluster-wide settings.
# MAGIC - **Pros**:
# MAGIC   - Simplified management for the entire cluster.
# MAGIC   - Suitable for multi-user environments.
# MAGIC - **Cons**:
# MAGIC   - Changes affect all users in the cluster.
# MAGIC   - Needs secure cluster configuration.
# MAGIC - **Recommended Scenarios**:
# MAGIC   - Shared cluster environments.
# MAGIC   - Unified management and configuration scenarios.
# MAGIC - **Example**: Cluster-scoped authentication is typically configured via spark config
# MAGIC
# MAGIC ```py
# MAGIC   spark.databricks.cluster.profile singleNode
# MAGIC
# MAGIC   spark.master local[*, 4]
# MAGIC
# MAGIC   fs.azure.account.key.<storage_account>.blob.core.windows.net".format <storage_account_key>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Access via Cluster Scoped Authentication
# MAGIC
# MAGIC - **Description**: Using user's Azure AD credentials to pass through authentication, allowing user identity to be passed to Azure Storage.
# MAGIC - **Pros**:
# MAGIC   - High security with transparent user identity pass-through.
# MAGIC   - No need for extra credentials management.
# MAGIC - **Cons**:
# MAGIC   - Requires Azure AD integration with Databricks.
# MAGIC   - Specific subscription and Databricks support needed.
# MAGIC - **Recommended Scenarios**:
# MAGIC   - Multi-user environments with varied permissions and access controls.
# MAGIC   - Minimizing credential management overhead.
# MAGIC - **Example**: 
# MAGIC
# MAGIC ```py
# MAGIC   # Enable "Credential Passthrough" in Databricks cluster settings.
# MAGIC   # Grant role to the user in Azure Storage Account via IAM
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary 
# MAGIC | Method                         | Description | Pros | Cons | Recommended Scenarios | Example |
# MAGIC |--------------------------------|-------------|------|------|-----------------------|---------|
# MAGIC | **Access via Access Key** | Using the storage account access key to access data. | - Simple and quick setup. - Suitable for quick testing and development. | - Lower security. - Risky for large-scale use. | - Development and testing environments. - Temporary or one-time data access. | ```python storage_account_name = "your_storage_account_name" storage_account_key = "your_storage_account_key" spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name), storage_account_key) ``` |
# MAGIC | **Access via SAS Token** | Using a Shared Access Signature (SAS) token to restrict access. | - High flexibility with permissions and expiry. - Safer as access is time-limited and scoped. | - Requires periodic generation and management of SAS tokens. - Higher management complexity. | - Short-term access with restricted permissions. - Temporary data sharing and access. | ```python sas_token = "your_sas_token" storage_account_name = "your_storage_account_name" spark.conf.set("fs.azure.sas.your_container.{}.blob.core.windows.net".format(storage_account_name), sas_token) ``` |
# MAGIC | **Access via Service Principal** | Using Azure Active Directory (AAD) service principal for authentication. | - High security through AAD authentication and authorization. - Fine-grained access control. | - Requires additional setup for service principal and permissions. - More complex configuration and management. | - High-security requirements in production environments. - Large-scale enterprise environments needing strict access control. | ```python service_principal_id = "your_service_principal_id" service_principal_secret = "your_service_principal_secret" tenant_id = "your_tenant_id" storage_account_name = "your_storage_account_name" spark.conf.set("fs.azure.account.auth.type", "OAuth") spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") spark.conf.set("fs.azure.account.oauth2.client.id", service_principal_id) spark.conf.set("fs.azure.account.oauth2.client.secret", service_principal_secret) spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id)) ``` |
# MAGIC | **Access via Cluster Scoped Authentication** | Configuring authentication at the cluster level, typically for cluster-wide settings. | - Simplified management for the entire cluster. - Suitable for multi-user environments. | - Changes affect all users in the cluster. - Needs secure cluster configuration. | - Shared cluster environments. - Unified management and configuration scenarios. | Typically configured via cluster init scripts or cluster configuration. |
# MAGIC | **Access via Credential Passthrough** | Using user's Azure AD credentials to pass through authentication. | - High security with transparent user identity pass-through. - No need for extra credentials management. | - Requires Azure AD integration with Databricks. - Specific subscription and Databricks support needed. | - Multi-user environments with varied permissions and access controls. - Minimizing credential management overhead. | ```python # Configure credential passthrough # Enable "Credential Passthrough" in Databricks cluster settings. # Ensure users are authenticated via Azure AD. dbutils.fs.ls("abfss://your_container@your_storage_account.dfs.core.windows.net/") ``` |
# MAGIC
