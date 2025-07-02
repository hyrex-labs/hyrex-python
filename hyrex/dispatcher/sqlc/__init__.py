"""
Auto-generated file by generate-python-exports.py
This file exports all SQLC generated query functions
Generated on: 2025-07-02T10:51:38.223763
"""

# Import all models
from .models import *

# Tables
from ._01_create_executor_table import Querier as _01_create_executor_table_Querier, AsyncQuerier as _01_create_executor_table_AsyncQuerier
from ._02_create_hyrex_app_table import Querier as _02_create_hyrex_app_table_Querier, AsyncQuerier as _02_create_hyrex_app_table_AsyncQuerier
from ._03_create_hyrex_cron_job_table import Querier as _03_create_hyrex_cron_job_table_Querier, AsyncQuerier as _03_create_hyrex_cron_job_table_AsyncQuerier
from ._04_create_hyrex_scheduler_lock_table import Querier as _04_create_hyrex_scheduler_lock_table_Querier, AsyncQuerier as _04_create_hyrex_scheduler_lock_table_AsyncQuerier
from ._05_create_hyrex_stats_task_status_counts_table import Querier as _05_create_hyrex_stats_task_status_counts_table_Querier, AsyncQuerier as _05_create_hyrex_stats_task_status_counts_table_AsyncQuerier
from ._06_create_hyrex_stats_task_status_counts_table_indexes import Querier as _06_create_hyrex_stats_task_status_counts_table_indexes_Querier, AsyncQuerier as _06_create_hyrex_stats_task_status_counts_table_indexes_AsyncQuerier
from ._07_create_hyrex_task_def_table import Querier as _07_create_hyrex_task_def_table_Querier, AsyncQuerier as _07_create_hyrex_task_def_table_AsyncQuerier
from ._08_create_workflow_table import Querier as _08_create_workflow_table_Querier, AsyncQuerier as _08_create_workflow_table_AsyncQuerier
from ._09_create_system_log_table import Querier as _09_create_system_log_table_Querier, AsyncQuerier as _09_create_system_log_table_AsyncQuerier
from ._10_create_workflow_run_table import Querier as _10_create_workflow_run_table_Querier, AsyncQuerier as _10_create_workflow_run_table_AsyncQuerier
from ._11_create_hyrex_cron_job_run_details_table import Querier as _11_create_hyrex_cron_job_run_details_table_Querier, AsyncQuerier as _11_create_hyrex_cron_job_run_details_table_AsyncQuerier
from ._12_create_hyrex_task_run_table import Querier as _12_create_hyrex_task_run_table_Querier, AsyncQuerier as _12_create_hyrex_task_run_table_AsyncQuerier
from ._13_create_hyrex_task_run_table_indexes import Querier as _13_create_hyrex_task_run_table_indexes_Querier, AsyncQuerier as _13_create_hyrex_task_run_table_indexes_AsyncQuerier
from ._14_create_results_table import Querier as _14_create_results_table_Querier, AsyncQuerier as _14_create_results_table_AsyncQuerier
from ._15_create_hyrex_kv_table import Querier as _15_create_hyrex_kv_table_Querier, AsyncQuerier as _15_create_hyrex_kv_table_AsyncQuerier
from ._16_create_hype_cron_job_table import Querier as _16_create_hype_cron_job_table_Querier, AsyncQuerier as _16_create_hype_cron_job_table_AsyncQuerier
from ._17_create_hype_cron_job_run_details_table import Querier as _17_create_hype_cron_job_run_details_table_Querier, AsyncQuerier as _17_create_hype_cron_job_run_details_table_AsyncQuerier

# Scheduler
from .acquire_scheduler_lock import Querier as acquire_scheduler_lock_Querier, AsyncQuerier as acquire_scheduler_lock_AsyncQuerier
from .release_scheduler_lock import Querier as release_scheduler_lock_Querier, AsyncQuerier as release_scheduler_lock_AsyncQuerier

# Workflow Run
from .advance_workflow_run_func import Querier as advance_workflow_run_func_Querier, AsyncQuerier as advance_workflow_run_func_AsyncQuerier
from .create_workflow_run import Querier as create_workflow_run_Querier, AsyncQuerier as create_workflow_run_AsyncQuerier
from .insert_workflow_run import Querier as insert_workflow_run_Querier, AsyncQuerier as insert_workflow_run_AsyncQuerier
from .set_workflow_run_status_based_on_task_runs import Querier as set_workflow_run_status_based_on_task_runs_Querier, AsyncQuerier as set_workflow_run_status_based_on_task_runs_AsyncQuerier
from .skip_waiting_task_for_workflow_run_id import Querier as skip_waiting_task_for_workflow_run_id_Querier, AsyncQuerier as skip_waiting_task_for_workflow_run_id_AsyncQuerier
from .update_workflow_run_status import Querier as update_workflow_run_status_Querier, AsyncQuerier as update_workflow_run_status_AsyncQuerier

# Executor
from .batch_update_heartbeat_log import Querier as batch_update_heartbeat_log_Querier, AsyncQuerier as batch_update_heartbeat_log_AsyncQuerier
from .batch_update_heartbeat_on_executors import Querier as batch_update_heartbeat_on_executors_Querier, AsyncQuerier as batch_update_heartbeat_on_executors_AsyncQuerier
from .disconnect_executor import Querier as disconnect_executor_Querier, AsyncQuerier as disconnect_executor_AsyncQuerier
from .mark_lost_executors import Querier as mark_lost_executors_Querier, AsyncQuerier as mark_lost_executors_AsyncQuerier
from .register_executor import Querier as register_executor_Querier, AsyncQuerier as register_executor_AsyncQuerier
from .update_executor_stats import Querier as update_executor_stats_Querier, AsyncQuerier as update_executor_stats_AsyncQuerier
from .update_queues_on_executor import Querier as update_queues_on_executor_Querier, AsyncQuerier as update_queues_on_executor_AsyncQuerier

# Hype
from .claim_queued_hype_cron_job_runs import Querier as claim_queued_hype_cron_job_runs_Querier, AsyncQuerier as claim_queued_hype_cron_job_runs_AsyncQuerier
from .count_queued_hype_cron_job_runs import Querier as count_queued_hype_cron_job_runs_Querier, AsyncQuerier as count_queued_hype_cron_job_runs_AsyncQuerier
from .create_hype_cron_job_run_details import Querier as create_hype_cron_job_run_details_Querier, AsyncQuerier as create_hype_cron_job_run_details_AsyncQuerier
from .get_active_hype_cron_jobs import Querier as get_active_hype_cron_jobs_Querier, AsyncQuerier as get_active_hype_cron_jobs_AsyncQuerier
from .get_hype_cron_job_by_name import Querier as get_hype_cron_job_by_name_Querier, AsyncQuerier as get_hype_cron_job_by_name_AsyncQuerier
from .register_hype_cron_job import Querier as register_hype_cron_job_Querier, AsyncQuerier as register_hype_cron_job_AsyncQuerier
from .update_hype_cron_job_confirmed_until import Querier as update_hype_cron_job_confirmed_until_Querier, AsyncQuerier as update_hype_cron_job_confirmed_until_AsyncQuerier
from .update_hype_cron_job_run_status import Querier as update_hype_cron_job_run_status_Querier, AsyncQuerier as update_hype_cron_job_run_status_AsyncQuerier

# Task Run
from .conditionally_retry_task import Querier as conditionally_retry_task_Querier, AsyncQuerier as conditionally_retry_task_AsyncQuerier
from .fetch_active_queue_names import Querier as fetch_active_queue_names_Querier, AsyncQuerier as fetch_active_queue_names_AsyncQuerier
from .fetch_result import Querier as fetch_result_Querier, AsyncQuerier as fetch_result_AsyncQuerier
from .fetch_task import Querier as fetch_task_Querier, AsyncQuerier as fetch_task_AsyncQuerier
from .fetch_task_with_concurrency_limit import Querier as fetch_task_with_concurrency_limit_Querier, AsyncQuerier as fetch_task_with_concurrency_limit_AsyncQuerier
from .save_result import Querier as save_result_Querier, AsyncQuerier as save_result_AsyncQuerier
from .set_log_link import Querier as set_log_link_Querier, AsyncQuerier as set_log_link_AsyncQuerier
from .transition_task_state import Querier as transition_task_state_Querier, AsyncQuerier as transition_task_state_AsyncQuerier

# Functions
from .create_advance_workflow_run_func import Querier as create_advance_workflow_run_func_Querier, AsyncQuerier as create_advance_workflow_run_func_AsyncQuerier
from .create_conditionally_retry_task_func import Querier as create_conditionally_retry_task_func_Querier, AsyncQuerier as create_conditionally_retry_task_func_AsyncQuerier
from .create_execute_queued_cron_job_func import Querier as create_execute_queued_cron_job_func_Querier, AsyncQuerier as create_execute_queued_cron_job_func_AsyncQuerier
from .create_schedule_cron_job_runs_func import Querier as create_schedule_cron_job_runs_func_Querier, AsyncQuerier as create_schedule_cron_job_runs_func_AsyncQuerier
from .create_set_workflow_run_status_based_on_task_runs_func import Querier as create_set_workflow_run_status_based_on_task_runs_func_Querier, AsyncQuerier as create_set_workflow_run_status_based_on_task_runs_func_AsyncQuerier
from .create_task_run import Querier as create_task_run_Querier, AsyncQuerier as create_task_run_AsyncQuerier
from .create_transition_task_run_state_func import Querier as create_transition_task_run_state_func_Querier, AsyncQuerier as create_transition_task_run_state_func_AsyncQuerier
from .create_uuid7_func import Querier as create_uuid7_func_Querier, AsyncQuerier as create_uuid7_func_AsyncQuerier
from .create_workflow_trigger_func import Querier as create_workflow_trigger_func_Querier, AsyncQuerier as create_workflow_trigger_func_AsyncQuerier

# Cron Job
from .create_cron_job_for_sql_query import Querier as create_cron_job_for_sql_query_Querier, AsyncQuerier as create_cron_job_for_sql_query_AsyncQuerier
from .create_cron_job_for_task import Querier as create_cron_job_for_task_Querier, AsyncQuerier as create_cron_job_for_task_AsyncQuerier
from .pull_active_cron_expressions import Querier as pull_active_cron_expressions_Querier, AsyncQuerier as pull_active_cron_expressions_AsyncQuerier
from .schedule_cron_job_runs_json import Querier as schedule_cron_job_runs_json_Querier, AsyncQuerier as schedule_cron_job_runs_json_AsyncQuerier
from .trigger_execute_queued_cron_job import Querier as trigger_execute_queued_cron_job_Querier, AsyncQuerier as trigger_execute_queued_cron_job_AsyncQuerier
from .turn_off_cron_for_task import Querier as turn_off_cron_for_task_Querier, AsyncQuerier as turn_off_cron_for_task_AsyncQuerier
from .update_cron_job_confirmation_ts import Querier as update_cron_job_confirmation_ts_Querier, AsyncQuerier as update_cron_job_confirmation_ts_AsyncQuerier

# Enums
from .create_cron_job_status_enum import Querier as create_cron_job_status_enum_Querier, AsyncQuerier as create_cron_job_status_enum_AsyncQuerier
from .create_executor_status_enum import Querier as create_executor_status_enum_Querier, AsyncQuerier as create_executor_status_enum_AsyncQuerier
from .create_hype_command_type_enum import Querier as create_hype_command_type_enum_Querier, AsyncQuerier as create_hype_command_type_enum_AsyncQuerier
from .create_hype_cron_job_status_enum import Querier as create_hype_cron_job_status_enum_Querier, AsyncQuerier as create_hype_cron_job_status_enum_AsyncQuerier
from .create_job_source_type_enum import Querier as create_job_source_type_enum_Querier, AsyncQuerier as create_job_source_type_enum_AsyncQuerier
from .create_task_run_status_enum import Querier as create_task_run_status_enum_Querier, AsyncQuerier as create_task_run_status_enum_AsyncQuerier
from .create_workflow_run_status_enum import Querier as create_workflow_run_status_enum_Querier, AsyncQuerier as create_workflow_run_status_enum_AsyncQuerier

# Stats
from .fill_historical_task_status_counts_table import Querier as fill_historical_task_status_counts_table_Querier, AsyncQuerier as fill_historical_task_status_counts_table_AsyncQuerier
from .insert_single_task_status_counts_row import Querier as insert_single_task_status_counts_row_Querier, AsyncQuerier as insert_single_task_status_counts_row_AsyncQuerier
from .trim_task_stats import Querier as trim_task_stats_Querier, AsyncQuerier as trim_task_stats_AsyncQuerier

# Task Def
from .get_all_task_defs import Querier as get_all_task_defs_Querier, AsyncQuerier as get_all_task_defs_AsyncQuerier
from .get_task_def import Querier as get_task_def_Querier, AsyncQuerier as get_task_def_AsyncQuerier
from .register_task_def import Querier as register_task_def_Querier, AsyncQuerier as register_task_def_AsyncQuerier

# Dashboard
from .get_app_name import Querier as get_app_name_Querier, AsyncQuerier as get_app_name_AsyncQuerier
from .get_cron_job_run_details import Querier as get_cron_job_run_details_Querier, AsyncQuerier as get_cron_job_run_details_AsyncQuerier
from .get_cron_jobs_paginated import Querier as get_cron_jobs_paginated_Querier, AsyncQuerier as get_cron_jobs_paginated_AsyncQuerier
from .get_executor_by_id import Querier as get_executor_by_id_Querier, AsyncQuerier as get_executor_by_id_AsyncQuerier
from .get_executors_paginated import Querier as get_executors_paginated_Querier, AsyncQuerier as get_executors_paginated_AsyncQuerier
from .get_project_stats import Querier as get_project_stats_Querier, AsyncQuerier as get_project_stats_AsyncQuerier
from .get_task_attempts_by_durable_id import Querier as get_task_attempts_by_durable_id_Querier, AsyncQuerier as get_task_attempts_by_durable_id_AsyncQuerier
from .get_task_by_name import Querier as get_task_by_name_Querier, AsyncQuerier as get_task_by_name_AsyncQuerier
from .get_task_run_by_id import Querier as get_task_run_by_id_Querier, AsyncQuerier as get_task_run_by_id_AsyncQuerier
from .get_task_runs_by_status_paginated import Querier as get_task_runs_by_status_paginated_Querier, AsyncQuerier as get_task_runs_by_status_paginated_AsyncQuerier
from .get_task_runs_paginated import Querier as get_task_runs_paginated_Querier, AsyncQuerier as get_task_runs_paginated_AsyncQuerier
from .get_tasks_paginated import Querier as get_tasks_paginated_Querier, AsyncQuerier as get_tasks_paginated_AsyncQuerier
from .get_workflow_by_name import Querier as get_workflow_by_name_Querier, AsyncQuerier as get_workflow_by_name_AsyncQuerier
from .get_workflow_run_by_id import Querier as get_workflow_run_by_id_Querier, AsyncQuerier as get_workflow_run_by_id_AsyncQuerier
from .get_workflow_run_task_runs import Querier as get_workflow_run_task_runs_Querier, AsyncQuerier as get_workflow_run_task_runs_AsyncQuerier
from .get_workflow_runs_paginated import Querier as get_workflow_runs_paginated_Querier, AsyncQuerier as get_workflow_runs_paginated_AsyncQuerier
from .get_workflows_paginated import Querier as get_workflows_paginated_Querier, AsyncQuerier as get_workflows_paginated_AsyncQuerier

# Kv
from .kv_delete_value import Querier as kv_delete_value_Querier, AsyncQuerier as kv_delete_value_AsyncQuerier
from .kv_flush_keys import Querier as kv_flush_keys_Querier, AsyncQuerier as kv_flush_keys_AsyncQuerier
from .kv_get_value import Querier as kv_get_value_Querier, AsyncQuerier as kv_get_value_AsyncQuerier
from .kv_list_keys_paginated import Querier as kv_list_keys_paginated_Querier, AsyncQuerier as kv_list_keys_paginated_AsyncQuerier
from .kv_set_value import Querier as kv_set_value_Querier, AsyncQuerier as kv_set_value_AsyncQuerier

# App
from .register_app_info import Querier as register_app_info_Querier, AsyncQuerier as register_app_info_AsyncQuerier

# Workflow
from .register_workflow import Querier as register_workflow_Querier, AsyncQuerier as register_workflow_AsyncQuerier

# Durability
from .set_executor_to_lost_if_no_heartbeat import Querier as set_executor_to_lost_if_no_heartbeat_Querier, AsyncQuerier as set_executor_to_lost_if_no_heartbeat_AsyncQuerier
from .set_orphaned_task_execution_to_lost_and_retry import Querier as set_orphaned_task_execution_to_lost_and_retry_Querier, AsyncQuerier as set_orphaned_task_execution_to_lost_and_retry_AsyncQuerier

# Generated wrapper functions for SQLC queries

# Synchronous functions
def create_executor_table_sync(client, *args, **kwargs):
    querier = _01_create_executor_table_Querier(client)
    from ._01_create_executor_table import CreateExecutorTableParams
    params = CreateExecutorTableParams()
    return querier.create_executor_table(params)

def create_app_table_sync(client, *args, **kwargs):
    querier = _02_create_hyrex_app_table_Querier(client)
    from ._02_create_hyrex_app_table import CreateAppTableParams
    params = CreateAppTableParams()
    return querier.create_app_table(params)

def create_cron_job_table_sync(client, *args, **kwargs):
    querier = _03_create_hyrex_cron_job_table_Querier(client)
    from ._03_create_hyrex_cron_job_table import CreateCronJobTableParams
    params = CreateCronJobTableParams()
    return querier.create_cron_job_table(params)

def create_scheduler_lock_table_sync(client, *args, **kwargs):
    querier = _04_create_hyrex_scheduler_lock_table_Querier(client)
    from ._04_create_hyrex_scheduler_lock_table import CreateSchedulerLockTableParams
    params = CreateSchedulerLockTableParams()
    return querier.create_scheduler_lock_table(params)

def create_stats_task_status_counts_table_sync(client, *args, **kwargs):
    querier = _05_create_hyrex_stats_task_status_counts_table_Querier(client)
    from ._05_create_hyrex_stats_task_status_counts_table import CreateStatsTaskStatusCountsTableParams
    params = CreateStatsTaskStatusCountsTableParams()
    return querier.create_stats_task_status_counts_table(params)

def create_stats_task_status_counts_table_indexes_sync(client, *args, **kwargs):
    querier = _06_create_hyrex_stats_task_status_counts_table_indexes_Querier(client)
    from ._06_create_hyrex_stats_task_status_counts_table_indexes import CreateStatsTaskStatusCountsTableIndexesParams
    params = CreateStatsTaskStatusCountsTableIndexesParams()
    return querier.create_stats_task_status_counts_table_indexes(params)

def create_task_def_table_sync(client, *args, **kwargs):
    querier = _07_create_hyrex_task_def_table_Querier(client)
    from ._07_create_hyrex_task_def_table import CreateTaskDefTableParams
    params = CreateTaskDefTableParams()
    return querier.create_task_def_table(params)

def create_workflow_table_sync(client, *args, **kwargs):
    querier = _08_create_workflow_table_Querier(client)
    from ._08_create_workflow_table import CreateWorkflowTableParams
    params = CreateWorkflowTableParams()
    return querier.create_workflow_table(params)

def create_system_log_table_sync(client, *args, **kwargs):
    querier = _09_create_system_log_table_Querier(client)
    from ._09_create_system_log_table import CreateSystemLogTableParams
    params = CreateSystemLogTableParams()
    return querier.create_system_log_table(params)

def create_workflow_run_table_sync(client, *args, **kwargs):
    querier = _10_create_workflow_run_table_Querier(client)
    from ._10_create_workflow_run_table import CreateWorkflowRunTableParams
    params = CreateWorkflowRunTableParams()
    return querier.create_workflow_run_table(params)

def create_cron_job_run_details_table_sync(client, *args, **kwargs):
    querier = _11_create_hyrex_cron_job_run_details_table_Querier(client)
    from ._11_create_hyrex_cron_job_run_details_table import CreateCronJobRunDetailsTableParams
    params = CreateCronJobRunDetailsTableParams()
    return querier.create_cron_job_run_details_table(params)

def create_task_run_table_sync(client, *args, **kwargs):
    querier = _12_create_hyrex_task_run_table_Querier(client)
    from ._12_create_hyrex_task_run_table import CreateTaskRunTableParams
    params = CreateTaskRunTableParams()
    return querier.create_task_run_table(params)

def create_task_run_table_indexes_sync(client, *args, **kwargs):
    querier = _13_create_hyrex_task_run_table_indexes_Querier(client)
    from ._13_create_hyrex_task_run_table_indexes import CreateTaskRunTableIndexesParams
    params = CreateTaskRunTableIndexesParams()
    return querier.create_task_run_table_indexes(params)

def create_results_table_sync(client, *args, **kwargs):
    querier = _14_create_results_table_Querier(client)
    from ._14_create_results_table import CreateResultsTableParams
    params = CreateResultsTableParams()
    return querier.create_results_table(params)

def create_hyrex_kv_table_sync(client, *args, **kwargs):
    querier = _15_create_hyrex_kv_table_Querier(client)
    from ._15_create_hyrex_kv_table import CreateHyrexKvTableParams
    params = CreateHyrexKvTableParams()
    return querier.create_hyrex_kv_table(params)

def create_hype_cron_job_table_sync(client, *args, **kwargs):
    querier = _16_create_hype_cron_job_table_Querier(client)
    from ._16_create_hype_cron_job_table import CreateHypeCronJobTableParams
    params = CreateHypeCronJobTableParams()
    return querier.create_hype_cron_job_table(params)

def create_hype_cron_job_run_details_table_sync(client, *args, **kwargs):
    querier = _17_create_hype_cron_job_run_details_table_Querier(client)
    from ._17_create_hype_cron_job_run_details_table import CreateHypeCronJobRunDetailsTableParams
    params = CreateHypeCronJobRunDetailsTableParams()
    return querier.create_hype_cron_job_run_details_table(params)

def acquire_scheduler_lock_sync(client, *args, **kwargs):
    querier = acquire_scheduler_lock_Querier(client)
    return querier.acquire_scheduler_lock(*args, **kwargs)

def advance_workflow_run_func_sync(client, *args, **kwargs):
    querier = advance_workflow_run_func_Querier(client)
    return querier.advance_workflow_run_func(*args, **kwargs)

def batch_update_heartbeat_log_sync(client, *args, **kwargs):
    querier = batch_update_heartbeat_log_Querier(client)
    return querier.batch_update_heartbeat_log(*args, **kwargs)

def batch_update_heartbeat_on_executors_sync(client, *args, **kwargs):
    querier = batch_update_heartbeat_on_executors_Querier(client)
    return querier.batch_update_heartbeat_on_executors(*args, **kwargs)

def claim_queued_hype_cron_job_runs_sync(client, *args, **kwargs):
    querier = claim_queued_hype_cron_job_runs_Querier(client)
    return querier.claim_queued_hype_cron_job_runs(*args, **kwargs)

def conditionally_retry_task_sync(client, *args, **kwargs):
    querier = conditionally_retry_task_Querier(client)
    return querier.conditionally_retry_task(*args, **kwargs)

def count_queued_hype_cron_job_runs_sync(client, *args, **kwargs):
    querier = count_queued_hype_cron_job_runs_Querier(client)
    return querier.count_queued_hype_cron_job_runs(*args, **kwargs)

def create_advance_workflow_run_function_sync(client, *args, **kwargs):
    querier = create_advance_workflow_run_func_Querier(client)
    from .create_advance_workflow_run_func import CreateAdvanceWorkflowRunFunctionParams
    params = CreateAdvanceWorkflowRunFunctionParams()
    return querier.create_advance_workflow_run_function(params)

def create_conditionally_retry_task_func_sync(client, *args, **kwargs):
    querier = create_conditionally_retry_task_func_Querier(client)
    from .create_conditionally_retry_task_func import CreateConditionallyRetryTaskFuncParams
    params = CreateConditionallyRetryTaskFuncParams()
    return querier.create_conditionally_retry_task_func(params)

def create_cron_job_for_sql_query_sync(client, *args, **kwargs):
    querier = create_cron_job_for_sql_query_Querier(client)
    return querier.create_cron_job_for_sql_query(*args, **kwargs)

def create_cron_job_for_task_sync(client, *args, **kwargs):
    querier = create_cron_job_for_task_Querier(client)
    return querier.create_cron_job_for_task(*args, **kwargs)

def create_cron_job_status_enum_sync(client, *args, **kwargs):
    querier = create_cron_job_status_enum_Querier(client)
    from .create_cron_job_status_enum import CreateCronJobStatusEnumParams
    params = CreateCronJobStatusEnumParams()
    return querier.create_cron_job_status_enum(params)

def create_execute_queued_cron_job_function_sync(client, *args, **kwargs):
    querier = create_execute_queued_cron_job_func_Querier(client)
    from .create_execute_queued_cron_job_func import CreateExecuteQueuedCronJobFunctionParams
    params = CreateExecuteQueuedCronJobFunctionParams()
    return querier.create_execute_queued_cron_job_function(params)

def create_executor_status_enum_sync(client, *args, **kwargs):
    querier = create_executor_status_enum_Querier(client)
    from .create_executor_status_enum import CreateExecutorStatusEnumParams
    params = CreateExecutorStatusEnumParams()
    return querier.create_executor_status_enum(params)

def create_hype_command_type_enum_sync(client, *args, **kwargs):
    querier = create_hype_command_type_enum_Querier(client)
    from .create_hype_command_type_enum import CreateHypeCommandTypeEnumParams
    params = CreateHypeCommandTypeEnumParams()
    return querier.create_hype_command_type_enum(params)

def create_hype_cron_job_run_details_sync(client, *args, **kwargs):
    querier = create_hype_cron_job_run_details_Querier(client)
    return querier.create_hype_cron_job_run_details(*args, **kwargs)

def create_hype_cron_job_status_enum_sync(client, *args, **kwargs):
    querier = create_hype_cron_job_status_enum_Querier(client)
    from .create_hype_cron_job_status_enum import CreateHypeCronJobStatusEnumParams
    params = CreateHypeCronJobStatusEnumParams()
    return querier.create_hype_cron_job_status_enum(params)

def create_job_source_type_enum_sync(client, *args, **kwargs):
    querier = create_job_source_type_enum_Querier(client)
    from .create_job_source_type_enum import CreateJobSourceTypeEnumParams
    params = CreateJobSourceTypeEnumParams()
    return querier.create_job_source_type_enum(params)

def create_schedule_cron_job_runs_func_sync(client, *args, **kwargs):
    querier = create_schedule_cron_job_runs_func_Querier(client)
    from .create_schedule_cron_job_runs_func import CreateScheduleCronJobRunsFuncParams
    params = CreateScheduleCronJobRunsFuncParams()
    return querier.create_schedule_cron_job_runs_func(params)

def create_set_workflow_run_status_based_on_task_runs_function_sync(client, *args, **kwargs):
    querier = create_set_workflow_run_status_based_on_task_runs_func_Querier(client)
    from .create_set_workflow_run_status_based_on_task_runs_func import CreateSetWorkflowRunStatusBasedOnTaskRunsFunctionParams
    params = CreateSetWorkflowRunStatusBasedOnTaskRunsFunctionParams()
    return querier.create_set_workflow_run_status_based_on_task_runs_function(params)

def create_task_run_sync(client, *args, **kwargs):
    querier = create_task_run_Querier(client)
    return querier.create_task_run(*args, **kwargs)

def create_task_run_function_sync(client, *args, **kwargs):
    querier = create_task_run_Querier(client)
    from .create_task_run import CreateTaskRunFunctionParams
    params = CreateTaskRunFunctionParams()
    return querier.create_task_run_function(params)

def create_task_run_status_enum_sync(client, *args, **kwargs):
    querier = create_task_run_status_enum_Querier(client)
    from .create_task_run_status_enum import CreateTaskRunStatusEnumParams
    params = CreateTaskRunStatusEnumParams()
    return querier.create_task_run_status_enum(params)

def create_transition_task_run_state_func_sync(client, *args, **kwargs):
    querier = create_transition_task_run_state_func_Querier(client)
    from .create_transition_task_run_state_func import CreateTransitionTaskRunStateFuncParams
    params = CreateTransitionTaskRunStateFuncParams()
    return querier.create_transition_task_run_state_func(params)

def create_uuid7_function_sync(client, *args, **kwargs):
    querier = create_uuid7_func_Querier(client)
    from .create_uuid7_func import CreateUuid7FunctionParams
    params = CreateUuid7FunctionParams()
    return querier.create_uuid7_function(params)

def create_workflow_run_sync(client, *args, **kwargs):
    querier = create_workflow_run_Querier(client)
    return querier.create_workflow_run(*args, **kwargs)

def create_workflow_run_status_enum_sync(client, *args, **kwargs):
    querier = create_workflow_run_status_enum_Querier(client)
    from .create_workflow_run_status_enum import CreateWorkflowRunStatusEnumParams
    params = CreateWorkflowRunStatusEnumParams()
    return querier.create_workflow_run_status_enum(params)

def create_workflow_trigger_sync(client, *args, **kwargs):
    querier = create_workflow_trigger_func_Querier(client)
    from .create_workflow_trigger_func import CreateWorkflowTriggerParams
    params = CreateWorkflowTriggerParams()
    return querier.create_workflow_trigger(params)

def disconnect_executor_sync(client, *args, **kwargs):
    querier = disconnect_executor_Querier(client)
    return querier.disconnect_executor(*args, **kwargs)

def fetch_active_queue_names_sync(client, *args, **kwargs):
    querier = fetch_active_queue_names_Querier(client)
    return querier.fetch_active_queue_names(*args, **kwargs)

def fetch_result_sync(client, *args, **kwargs):
    querier = fetch_result_Querier(client)
    return querier.fetch_result(*args, **kwargs)

def fetch_task_sync(client, *args, **kwargs):
    querier = fetch_task_Querier(client)
    return querier.fetch_task(*args, **kwargs)

def fetch_task_with_concurrency_limit_sync(client, *args, **kwargs):
    querier = fetch_task_with_concurrency_limit_Querier(client)
    return querier.fetch_task_with_concurrency_limit(*args, **kwargs)

def fill_historical_task_status_counts_table_sync(client, *args, **kwargs):
    querier = fill_historical_task_status_counts_table_Querier(client)
    return querier.fill_historical_task_status_counts_table(*args, **kwargs)

def get_active_hype_cron_jobs_sync(client, *args, **kwargs):
    querier = get_active_hype_cron_jobs_Querier(client)
    return querier.get_active_hype_cron_jobs(*args, **kwargs)

def get_all_task_defs_sync(client, *args, **kwargs):
    querier = get_all_task_defs_Querier(client)
    return querier.get_all_task_defs(*args, **kwargs)

def get_app_name_sync(client, *args, **kwargs):
    querier = get_app_name_Querier(client)
    return querier.get_app_name(*args, **kwargs)

def get_cron_job_run_details_sync(client, *args, **kwargs):
    querier = get_cron_job_run_details_Querier(client)
    return querier.get_cron_job_run_details(*args, **kwargs)

def get_cron_jobs_paginated_sync(client, *args, **kwargs):
    querier = get_cron_jobs_paginated_Querier(client)
    return querier.get_cron_jobs_paginated(*args, **kwargs)

def get_executor_by_id_sync(client, *args, **kwargs):
    querier = get_executor_by_id_Querier(client)
    return querier.get_executor_by_id(*args, **kwargs)

def get_executors_paginated_sync(client, *args, **kwargs):
    querier = get_executors_paginated_Querier(client)
    return querier.get_executors_paginated(*args, **kwargs)

def get_hype_cron_job_by_name_sync(client, *args, **kwargs):
    querier = get_hype_cron_job_by_name_Querier(client)
    return querier.get_hype_cron_job_by_name(*args, **kwargs)

def get_project_stats_sync(client, *args, **kwargs):
    querier = get_project_stats_Querier(client)
    return querier.get_project_stats(*args, **kwargs)

def get_task_attempts_by_durable_id_sync(client, *args, **kwargs):
    querier = get_task_attempts_by_durable_id_Querier(client)
    return querier.get_task_attempts_by_durable_id(*args, **kwargs)

def get_task_by_name_sync(client, *args, **kwargs):
    querier = get_task_by_name_Querier(client)
    return querier.get_task_by_name(*args, **kwargs)

def get_task_def_sync(client, *args, **kwargs):
    querier = get_task_def_Querier(client)
    return querier.get_task_def(*args, **kwargs)

def get_task_run_by_id_sync(client, *args, **kwargs):
    querier = get_task_run_by_id_Querier(client)
    return querier.get_task_run_by_id(*args, **kwargs)

def get_task_runs_by_status_paginated_sync(client, *args, **kwargs):
    querier = get_task_runs_by_status_paginated_Querier(client)
    return querier.get_task_runs_by_status_paginated(*args, **kwargs)

def get_task_runs_paginated_sync(client, *args, **kwargs):
    querier = get_task_runs_paginated_Querier(client)
    return querier.get_task_runs_paginated(*args, **kwargs)

def get_tasks_paginated_sync(client, *args, **kwargs):
    querier = get_tasks_paginated_Querier(client)
    return querier.get_tasks_paginated(*args, **kwargs)

def get_workflow_by_name_sync(client, *args, **kwargs):
    querier = get_workflow_by_name_Querier(client)
    return querier.get_workflow_by_name(*args, **kwargs)

def get_workflow_run_by_id_sync(client, *args, **kwargs):
    querier = get_workflow_run_by_id_Querier(client)
    return querier.get_workflow_run_by_id(*args, **kwargs)

def get_workflow_run_task_runs_sync(client, *args, **kwargs):
    querier = get_workflow_run_task_runs_Querier(client)
    return querier.get_workflow_run_task_runs(*args, **kwargs)

def get_workflow_runs_paginated_sync(client, *args, **kwargs):
    querier = get_workflow_runs_paginated_Querier(client)
    return querier.get_workflow_runs_paginated(*args, **kwargs)

def get_workflows_paginated_sync(client, *args, **kwargs):
    querier = get_workflows_paginated_Querier(client)
    return querier.get_workflows_paginated(*args, **kwargs)

def insert_single_task_status_counts_row_sync(client, *args, **kwargs):
    querier = insert_single_task_status_counts_row_Querier(client)
    return querier.insert_single_task_status_counts_row(*args, **kwargs)

def insert_workflow_run_sync(client, *args, **kwargs):
    querier = insert_workflow_run_Querier(client)
    return querier.insert_workflow_run(*args, **kwargs)

def delete_value_sync(client, *args, **kwargs):
    querier = kv_delete_value_Querier(client)
    return querier.delete_value(*args, **kwargs)

def flush_keys_sync(client, *args, **kwargs):
    querier = kv_flush_keys_Querier(client)
    return querier.flush_keys(*args, **kwargs)

def get_value_sync(client, *args, **kwargs):
    querier = kv_get_value_Querier(client)
    return querier.get_value(*args, **kwargs)

def list_keys_paginated_sync(client, *args, **kwargs):
    querier = kv_list_keys_paginated_Querier(client)
    return querier.list_keys_paginated(*args, **kwargs)

def set_value_sync(client, *args, **kwargs):
    querier = kv_set_value_Querier(client)
    return querier.set_value(*args, **kwargs)

def mark_lost_executors_sync(client, *args, **kwargs):
    querier = mark_lost_executors_Querier(client)
    return querier.mark_lost_executors(*args, **kwargs)

def pull_active_cron_expressions_sync(client, *args, **kwargs):
    querier = pull_active_cron_expressions_Querier(client)
    return querier.pull_active_cron_expressions(*args, **kwargs)

def register_app_info_sync(client, *args, **kwargs):
    querier = register_app_info_Querier(client)
    return querier.register_app_info(*args, **kwargs)

def register_executor_sync(client, *args, **kwargs):
    querier = register_executor_Querier(client)
    return querier.register_executor(*args, **kwargs)

def register_hype_cron_job_sync(client, *args, **kwargs):
    querier = register_hype_cron_job_Querier(client)
    return querier.register_hype_cron_job(*args, **kwargs)

def register_task_def_sync(client, *args, **kwargs):
    querier = register_task_def_Querier(client)
    return querier.register_task_def(*args, **kwargs)

def register_workflow_sync(client, *args, **kwargs):
    querier = register_workflow_Querier(client)
    return querier.register_workflow(*args, **kwargs)

def release_scheduler_lock_sync(client, *args, **kwargs):
    querier = release_scheduler_lock_Querier(client)
    return querier.release_scheduler_lock(*args, **kwargs)

def save_result_sync(client, *args, **kwargs):
    querier = save_result_Querier(client)
    return querier.save_result(*args, **kwargs)

def schedule_cron_job_runs_json_sync(client, *args, **kwargs):
    querier = schedule_cron_job_runs_json_Querier(client)
    return querier.schedule_cron_job_runs_json(*args, **kwargs)

def set_executor_to_lost_if_no_heartbeat_sync(client, *args, **kwargs):
    querier = set_executor_to_lost_if_no_heartbeat_Querier(client)
    return querier.set_executor_to_lost_if_no_heartbeat(*args, **kwargs)

def set_log_link_sync(client, *args, **kwargs):
    querier = set_log_link_Querier(client)
    return querier.set_log_link(*args, **kwargs)

def set_orphaned_task_execution_to_lost_and_retry_sync(client, *args, **kwargs):
    querier = set_orphaned_task_execution_to_lost_and_retry_Querier(client)
    return querier.set_orphaned_task_execution_to_lost_and_retry(*args, **kwargs)

def set_workflow_run_status_based_on_task_runs_sync(client, *args, **kwargs):
    querier = set_workflow_run_status_based_on_task_runs_Querier(client)
    return querier.set_workflow_run_status_based_on_task_runs(*args, **kwargs)

def skip_waiting_task_for_workflow_run_id_sync(client, *args, **kwargs):
    querier = skip_waiting_task_for_workflow_run_id_Querier(client)
    return querier.skip_waiting_task_for_workflow_run_id(*args, **kwargs)

def transition_task_state_sync(client, *args, **kwargs):
    querier = transition_task_state_Querier(client)
    return querier.transition_task_state(*args, **kwargs)

def trigger_execute_queued_cron_job_sync(client, *args, **kwargs):
    querier = trigger_execute_queued_cron_job_Querier(client)
    return querier.trigger_execute_queued_cron_job(*args, **kwargs)

def trim_task_stats_sync(client, *args, **kwargs):
    querier = trim_task_stats_Querier(client)
    return querier.trim_task_stats(*args, **kwargs)

def turn_off_cron_for_task_sync(client, *args, **kwargs):
    querier = turn_off_cron_for_task_Querier(client)
    return querier.turn_off_cron_for_task(*args, **kwargs)

def update_cron_job_confirmation_ts_sync(client, *args, **kwargs):
    querier = update_cron_job_confirmation_ts_Querier(client)
    return querier.update_cron_job_confirmation_ts(*args, **kwargs)

def update_executor_stats_sync(client, *args, **kwargs):
    querier = update_executor_stats_Querier(client)
    return querier.update_executor_stats(*args, **kwargs)

def update_hype_cron_job_confirmed_until_sync(client, *args, **kwargs):
    querier = update_hype_cron_job_confirmed_until_Querier(client)
    return querier.update_hype_cron_job_confirmed_until(*args, **kwargs)

def update_hype_cron_job_run_status_sync(client, *args, **kwargs):
    querier = update_hype_cron_job_run_status_Querier(client)
    return querier.update_hype_cron_job_run_status(*args, **kwargs)

def update_queues_on_executor_sync(client, *args, **kwargs):
    querier = update_queues_on_executor_Querier(client)
    return querier.update_queues_on_executor(*args, **kwargs)

def update_workflow_run_status_sync(client, *args, **kwargs):
    querier = update_workflow_run_status_Querier(client)
    return querier.update_workflow_run_status(*args, **kwargs)


# Asynchronous functions
async def create_executor_table_async(client, *args, **kwargs):
    querier = _01_create_executor_table_AsyncQuerier(client)
    from ._01_create_executor_table import CreateExecutorTableParams
    params = CreateExecutorTableParams()
    return await querier.create_executor_table(params)

async def create_app_table_async(client, *args, **kwargs):
    querier = _02_create_hyrex_app_table_AsyncQuerier(client)
    from ._02_create_hyrex_app_table import CreateAppTableParams
    params = CreateAppTableParams()
    return await querier.create_app_table(params)

async def create_cron_job_table_async(client, *args, **kwargs):
    querier = _03_create_hyrex_cron_job_table_AsyncQuerier(client)
    from ._03_create_hyrex_cron_job_table import CreateCronJobTableParams
    params = CreateCronJobTableParams()
    return await querier.create_cron_job_table(params)

async def create_scheduler_lock_table_async(client, *args, **kwargs):
    querier = _04_create_hyrex_scheduler_lock_table_AsyncQuerier(client)
    from ._04_create_hyrex_scheduler_lock_table import CreateSchedulerLockTableParams
    params = CreateSchedulerLockTableParams()
    return await querier.create_scheduler_lock_table(params)

async def create_stats_task_status_counts_table_async(client, *args, **kwargs):
    querier = _05_create_hyrex_stats_task_status_counts_table_AsyncQuerier(client)
    from ._05_create_hyrex_stats_task_status_counts_table import CreateStatsTaskStatusCountsTableParams
    params = CreateStatsTaskStatusCountsTableParams()
    return await querier.create_stats_task_status_counts_table(params)

async def create_stats_task_status_counts_table_indexes_async(client, *args, **kwargs):
    querier = _06_create_hyrex_stats_task_status_counts_table_indexes_AsyncQuerier(client)
    from ._06_create_hyrex_stats_task_status_counts_table_indexes import CreateStatsTaskStatusCountsTableIndexesParams
    params = CreateStatsTaskStatusCountsTableIndexesParams()
    return await querier.create_stats_task_status_counts_table_indexes(params)

async def create_task_def_table_async(client, *args, **kwargs):
    querier = _07_create_hyrex_task_def_table_AsyncQuerier(client)
    from ._07_create_hyrex_task_def_table import CreateTaskDefTableParams
    params = CreateTaskDefTableParams()
    return await querier.create_task_def_table(params)

async def create_workflow_table_async(client, *args, **kwargs):
    querier = _08_create_workflow_table_AsyncQuerier(client)
    from ._08_create_workflow_table import CreateWorkflowTableParams
    params = CreateWorkflowTableParams()
    return await querier.create_workflow_table(params)

async def create_system_log_table_async(client, *args, **kwargs):
    querier = _09_create_system_log_table_AsyncQuerier(client)
    from ._09_create_system_log_table import CreateSystemLogTableParams
    params = CreateSystemLogTableParams()
    return await querier.create_system_log_table(params)

async def create_workflow_run_table_async(client, *args, **kwargs):
    querier = _10_create_workflow_run_table_AsyncQuerier(client)
    from ._10_create_workflow_run_table import CreateWorkflowRunTableParams
    params = CreateWorkflowRunTableParams()
    return await querier.create_workflow_run_table(params)

async def create_cron_job_run_details_table_async(client, *args, **kwargs):
    querier = _11_create_hyrex_cron_job_run_details_table_AsyncQuerier(client)
    from ._11_create_hyrex_cron_job_run_details_table import CreateCronJobRunDetailsTableParams
    params = CreateCronJobRunDetailsTableParams()
    return await querier.create_cron_job_run_details_table(params)

async def create_task_run_table_async(client, *args, **kwargs):
    querier = _12_create_hyrex_task_run_table_AsyncQuerier(client)
    from ._12_create_hyrex_task_run_table import CreateTaskRunTableParams
    params = CreateTaskRunTableParams()
    return await querier.create_task_run_table(params)

async def create_task_run_table_indexes_async(client, *args, **kwargs):
    querier = _13_create_hyrex_task_run_table_indexes_AsyncQuerier(client)
    from ._13_create_hyrex_task_run_table_indexes import CreateTaskRunTableIndexesParams
    params = CreateTaskRunTableIndexesParams()
    return await querier.create_task_run_table_indexes(params)

async def create_results_table_async(client, *args, **kwargs):
    querier = _14_create_results_table_AsyncQuerier(client)
    from ._14_create_results_table import CreateResultsTableParams
    params = CreateResultsTableParams()
    return await querier.create_results_table(params)

async def create_hyrex_kv_table_async(client, *args, **kwargs):
    querier = _15_create_hyrex_kv_table_AsyncQuerier(client)
    from ._15_create_hyrex_kv_table import CreateHyrexKvTableParams
    params = CreateHyrexKvTableParams()
    return await querier.create_hyrex_kv_table(params)

async def create_hype_cron_job_table_async(client, *args, **kwargs):
    querier = _16_create_hype_cron_job_table_AsyncQuerier(client)
    from ._16_create_hype_cron_job_table import CreateHypeCronJobTableParams
    params = CreateHypeCronJobTableParams()
    return await querier.create_hype_cron_job_table(params)

async def create_hype_cron_job_run_details_table_async(client, *args, **kwargs):
    querier = _17_create_hype_cron_job_run_details_table_AsyncQuerier(client)
    from ._17_create_hype_cron_job_run_details_table import CreateHypeCronJobRunDetailsTableParams
    params = CreateHypeCronJobRunDetailsTableParams()
    return await querier.create_hype_cron_job_run_details_table(params)

async def acquire_scheduler_lock_async(client, *args, **kwargs):
    querier = acquire_scheduler_lock_AsyncQuerier(client)
    return await querier.acquire_scheduler_lock(*args, **kwargs)

async def advance_workflow_run_func_async(client, *args, **kwargs):
    querier = advance_workflow_run_func_AsyncQuerier(client)
    return await querier.advance_workflow_run_func(*args, **kwargs)

async def batch_update_heartbeat_log_async(client, *args, **kwargs):
    querier = batch_update_heartbeat_log_AsyncQuerier(client)
    return await querier.batch_update_heartbeat_log(*args, **kwargs)

async def batch_update_heartbeat_on_executors_async(client, *args, **kwargs):
    querier = batch_update_heartbeat_on_executors_AsyncQuerier(client)
    return await querier.batch_update_heartbeat_on_executors(*args, **kwargs)

async def claim_queued_hype_cron_job_runs_async(client, *args, **kwargs):
    querier = claim_queued_hype_cron_job_runs_AsyncQuerier(client)
    return await querier.claim_queued_hype_cron_job_runs(*args, **kwargs)

async def conditionally_retry_task_async(client, *args, **kwargs):
    querier = conditionally_retry_task_AsyncQuerier(client)
    return await querier.conditionally_retry_task(*args, **kwargs)

async def count_queued_hype_cron_job_runs_async(client, *args, **kwargs):
    querier = count_queued_hype_cron_job_runs_AsyncQuerier(client)
    return await querier.count_queued_hype_cron_job_runs(*args, **kwargs)

async def create_advance_workflow_run_function_async(client, *args, **kwargs):
    querier = create_advance_workflow_run_func_AsyncQuerier(client)
    from .create_advance_workflow_run_func import CreateAdvanceWorkflowRunFunctionParams
    params = CreateAdvanceWorkflowRunFunctionParams()
    return await querier.create_advance_workflow_run_function(params)

async def create_conditionally_retry_task_func_async(client, *args, **kwargs):
    querier = create_conditionally_retry_task_func_AsyncQuerier(client)
    from .create_conditionally_retry_task_func import CreateConditionallyRetryTaskFuncParams
    params = CreateConditionallyRetryTaskFuncParams()
    return await querier.create_conditionally_retry_task_func(params)

async def create_cron_job_for_sql_query_async(client, *args, **kwargs):
    querier = create_cron_job_for_sql_query_AsyncQuerier(client)
    return await querier.create_cron_job_for_sql_query(*args, **kwargs)

async def create_cron_job_for_task_async(client, *args, **kwargs):
    querier = create_cron_job_for_task_AsyncQuerier(client)
    return await querier.create_cron_job_for_task(*args, **kwargs)

async def create_cron_job_status_enum_async(client, *args, **kwargs):
    querier = create_cron_job_status_enum_AsyncQuerier(client)
    from .create_cron_job_status_enum import CreateCronJobStatusEnumParams
    params = CreateCronJobStatusEnumParams()
    return await querier.create_cron_job_status_enum(params)

async def create_execute_queued_cron_job_function_async(client, *args, **kwargs):
    querier = create_execute_queued_cron_job_func_AsyncQuerier(client)
    from .create_execute_queued_cron_job_func import CreateExecuteQueuedCronJobFunctionParams
    params = CreateExecuteQueuedCronJobFunctionParams()
    return await querier.create_execute_queued_cron_job_function(params)

async def create_executor_status_enum_async(client, *args, **kwargs):
    querier = create_executor_status_enum_AsyncQuerier(client)
    from .create_executor_status_enum import CreateExecutorStatusEnumParams
    params = CreateExecutorStatusEnumParams()
    return await querier.create_executor_status_enum(params)

async def create_hype_command_type_enum_async(client, *args, **kwargs):
    querier = create_hype_command_type_enum_AsyncQuerier(client)
    from .create_hype_command_type_enum import CreateHypeCommandTypeEnumParams
    params = CreateHypeCommandTypeEnumParams()
    return await querier.create_hype_command_type_enum(params)

async def create_hype_cron_job_run_details_async(client, *args, **kwargs):
    querier = create_hype_cron_job_run_details_AsyncQuerier(client)
    return await querier.create_hype_cron_job_run_details(*args, **kwargs)

async def create_hype_cron_job_status_enum_async(client, *args, **kwargs):
    querier = create_hype_cron_job_status_enum_AsyncQuerier(client)
    from .create_hype_cron_job_status_enum import CreateHypeCronJobStatusEnumParams
    params = CreateHypeCronJobStatusEnumParams()
    return await querier.create_hype_cron_job_status_enum(params)

async def create_job_source_type_enum_async(client, *args, **kwargs):
    querier = create_job_source_type_enum_AsyncQuerier(client)
    from .create_job_source_type_enum import CreateJobSourceTypeEnumParams
    params = CreateJobSourceTypeEnumParams()
    return await querier.create_job_source_type_enum(params)

async def create_schedule_cron_job_runs_func_async(client, *args, **kwargs):
    querier = create_schedule_cron_job_runs_func_AsyncQuerier(client)
    from .create_schedule_cron_job_runs_func import CreateScheduleCronJobRunsFuncParams
    params = CreateScheduleCronJobRunsFuncParams()
    return await querier.create_schedule_cron_job_runs_func(params)

async def create_set_workflow_run_status_based_on_task_runs_function_async(client, *args, **kwargs):
    querier = create_set_workflow_run_status_based_on_task_runs_func_AsyncQuerier(client)
    from .create_set_workflow_run_status_based_on_task_runs_func import CreateSetWorkflowRunStatusBasedOnTaskRunsFunctionParams
    params = CreateSetWorkflowRunStatusBasedOnTaskRunsFunctionParams()
    return await querier.create_set_workflow_run_status_based_on_task_runs_function(params)

async def create_task_run_async(client, *args, **kwargs):
    querier = create_task_run_AsyncQuerier(client)
    return await querier.create_task_run(*args, **kwargs)

async def create_task_run_function_async(client, *args, **kwargs):
    querier = create_task_run_AsyncQuerier(client)
    from .create_task_run import CreateTaskRunFunctionParams
    params = CreateTaskRunFunctionParams()
    return await querier.create_task_run_function(params)

async def create_task_run_status_enum_async(client, *args, **kwargs):
    querier = create_task_run_status_enum_AsyncQuerier(client)
    from .create_task_run_status_enum import CreateTaskRunStatusEnumParams
    params = CreateTaskRunStatusEnumParams()
    return await querier.create_task_run_status_enum(params)

async def create_transition_task_run_state_func_async(client, *args, **kwargs):
    querier = create_transition_task_run_state_func_AsyncQuerier(client)
    from .create_transition_task_run_state_func import CreateTransitionTaskRunStateFuncParams
    params = CreateTransitionTaskRunStateFuncParams()
    return await querier.create_transition_task_run_state_func(params)

async def create_uuid7_function_async(client, *args, **kwargs):
    querier = create_uuid7_func_AsyncQuerier(client)
    from .create_uuid7_func import CreateUuid7FunctionParams
    params = CreateUuid7FunctionParams()
    return await querier.create_uuid7_function(params)

async def create_workflow_run_async(client, *args, **kwargs):
    querier = create_workflow_run_AsyncQuerier(client)
    return await querier.create_workflow_run(*args, **kwargs)

async def create_workflow_run_status_enum_async(client, *args, **kwargs):
    querier = create_workflow_run_status_enum_AsyncQuerier(client)
    from .create_workflow_run_status_enum import CreateWorkflowRunStatusEnumParams
    params = CreateWorkflowRunStatusEnumParams()
    return await querier.create_workflow_run_status_enum(params)

async def create_workflow_trigger_async(client, *args, **kwargs):
    querier = create_workflow_trigger_func_AsyncQuerier(client)
    from .create_workflow_trigger_func import CreateWorkflowTriggerParams
    params = CreateWorkflowTriggerParams()
    return await querier.create_workflow_trigger(params)

async def disconnect_executor_async(client, *args, **kwargs):
    querier = disconnect_executor_AsyncQuerier(client)
    return await querier.disconnect_executor(*args, **kwargs)

async def fetch_active_queue_names_async(client, *args, **kwargs):
    querier = fetch_active_queue_names_AsyncQuerier(client)
    return await querier.fetch_active_queue_names(*args, **kwargs)

async def fetch_result_async(client, *args, **kwargs):
    querier = fetch_result_AsyncQuerier(client)
    return await querier.fetch_result(*args, **kwargs)

async def fetch_task_async(client, *args, **kwargs):
    querier = fetch_task_AsyncQuerier(client)
    return await querier.fetch_task(*args, **kwargs)

async def fetch_task_with_concurrency_limit_async(client, *args, **kwargs):
    querier = fetch_task_with_concurrency_limit_AsyncQuerier(client)
    return await querier.fetch_task_with_concurrency_limit(*args, **kwargs)

async def fill_historical_task_status_counts_table_async(client, *args, **kwargs):
    querier = fill_historical_task_status_counts_table_AsyncQuerier(client)
    return await querier.fill_historical_task_status_counts_table(*args, **kwargs)

async def get_active_hype_cron_jobs_async(client, *args, **kwargs):
    querier = get_active_hype_cron_jobs_AsyncQuerier(client)
    return await querier.get_active_hype_cron_jobs(*args, **kwargs)

async def get_all_task_defs_async(client, *args, **kwargs):
    querier = get_all_task_defs_AsyncQuerier(client)
    return await querier.get_all_task_defs(*args, **kwargs)

async def get_app_name_async(client, *args, **kwargs):
    querier = get_app_name_AsyncQuerier(client)
    return await querier.get_app_name(*args, **kwargs)

async def get_cron_job_run_details_async(client, *args, **kwargs):
    querier = get_cron_job_run_details_AsyncQuerier(client)
    return await querier.get_cron_job_run_details(*args, **kwargs)

async def get_cron_jobs_paginated_async(client, *args, **kwargs):
    querier = get_cron_jobs_paginated_AsyncQuerier(client)
    return await querier.get_cron_jobs_paginated(*args, **kwargs)

async def get_executor_by_id_async(client, *args, **kwargs):
    querier = get_executor_by_id_AsyncQuerier(client)
    return await querier.get_executor_by_id(*args, **kwargs)

async def get_executors_paginated_async(client, *args, **kwargs):
    querier = get_executors_paginated_AsyncQuerier(client)
    return await querier.get_executors_paginated(*args, **kwargs)

async def get_hype_cron_job_by_name_async(client, *args, **kwargs):
    querier = get_hype_cron_job_by_name_AsyncQuerier(client)
    return await querier.get_hype_cron_job_by_name(*args, **kwargs)

async def get_project_stats_async(client, *args, **kwargs):
    querier = get_project_stats_AsyncQuerier(client)
    return await querier.get_project_stats(*args, **kwargs)

async def get_task_attempts_by_durable_id_async(client, *args, **kwargs):
    querier = get_task_attempts_by_durable_id_AsyncQuerier(client)
    return await querier.get_task_attempts_by_durable_id(*args, **kwargs)

async def get_task_by_name_async(client, *args, **kwargs):
    querier = get_task_by_name_AsyncQuerier(client)
    return await querier.get_task_by_name(*args, **kwargs)

async def get_task_def_async(client, *args, **kwargs):
    querier = get_task_def_AsyncQuerier(client)
    return await querier.get_task_def(*args, **kwargs)

async def get_task_run_by_id_async(client, *args, **kwargs):
    querier = get_task_run_by_id_AsyncQuerier(client)
    return await querier.get_task_run_by_id(*args, **kwargs)

async def get_task_runs_by_status_paginated_async(client, *args, **kwargs):
    querier = get_task_runs_by_status_paginated_AsyncQuerier(client)
    return await querier.get_task_runs_by_status_paginated(*args, **kwargs)

async def get_task_runs_paginated_async(client, *args, **kwargs):
    querier = get_task_runs_paginated_AsyncQuerier(client)
    return await querier.get_task_runs_paginated(*args, **kwargs)

async def get_tasks_paginated_async(client, *args, **kwargs):
    querier = get_tasks_paginated_AsyncQuerier(client)
    return await querier.get_tasks_paginated(*args, **kwargs)

async def get_workflow_by_name_async(client, *args, **kwargs):
    querier = get_workflow_by_name_AsyncQuerier(client)
    return await querier.get_workflow_by_name(*args, **kwargs)

async def get_workflow_run_by_id_async(client, *args, **kwargs):
    querier = get_workflow_run_by_id_AsyncQuerier(client)
    return await querier.get_workflow_run_by_id(*args, **kwargs)

async def get_workflow_run_task_runs_async(client, *args, **kwargs):
    querier = get_workflow_run_task_runs_AsyncQuerier(client)
    return await querier.get_workflow_run_task_runs(*args, **kwargs)

async def get_workflow_runs_paginated_async(client, *args, **kwargs):
    querier = get_workflow_runs_paginated_AsyncQuerier(client)
    return await querier.get_workflow_runs_paginated(*args, **kwargs)

async def get_workflows_paginated_async(client, *args, **kwargs):
    querier = get_workflows_paginated_AsyncQuerier(client)
    return await querier.get_workflows_paginated(*args, **kwargs)

async def insert_single_task_status_counts_row_async(client, *args, **kwargs):
    querier = insert_single_task_status_counts_row_AsyncQuerier(client)
    return await querier.insert_single_task_status_counts_row(*args, **kwargs)

async def insert_workflow_run_async(client, *args, **kwargs):
    querier = insert_workflow_run_AsyncQuerier(client)
    return await querier.insert_workflow_run(*args, **kwargs)

async def delete_value_async(client, *args, **kwargs):
    querier = kv_delete_value_AsyncQuerier(client)
    return await querier.delete_value(*args, **kwargs)

async def flush_keys_async(client, *args, **kwargs):
    querier = kv_flush_keys_AsyncQuerier(client)
    return await querier.flush_keys(*args, **kwargs)

async def get_value_async(client, *args, **kwargs):
    querier = kv_get_value_AsyncQuerier(client)
    return await querier.get_value(*args, **kwargs)

async def list_keys_paginated_async(client, *args, **kwargs):
    querier = kv_list_keys_paginated_AsyncQuerier(client)
    return await querier.list_keys_paginated(*args, **kwargs)

async def set_value_async(client, *args, **kwargs):
    querier = kv_set_value_AsyncQuerier(client)
    return await querier.set_value(*args, **kwargs)

async def mark_lost_executors_async(client, *args, **kwargs):
    querier = mark_lost_executors_AsyncQuerier(client)
    return await querier.mark_lost_executors(*args, **kwargs)

async def pull_active_cron_expressions_async(client, *args, **kwargs):
    querier = pull_active_cron_expressions_AsyncQuerier(client)
    return await querier.pull_active_cron_expressions(*args, **kwargs)

async def register_app_info_async(client, *args, **kwargs):
    querier = register_app_info_AsyncQuerier(client)
    return await querier.register_app_info(*args, **kwargs)

async def register_executor_async(client, *args, **kwargs):
    querier = register_executor_AsyncQuerier(client)
    return await querier.register_executor(*args, **kwargs)

async def register_hype_cron_job_async(client, *args, **kwargs):
    querier = register_hype_cron_job_AsyncQuerier(client)
    return await querier.register_hype_cron_job(*args, **kwargs)

async def register_task_def_async(client, *args, **kwargs):
    querier = register_task_def_AsyncQuerier(client)
    return await querier.register_task_def(*args, **kwargs)

async def register_workflow_async(client, *args, **kwargs):
    querier = register_workflow_AsyncQuerier(client)
    return await querier.register_workflow(*args, **kwargs)

async def release_scheduler_lock_async(client, *args, **kwargs):
    querier = release_scheduler_lock_AsyncQuerier(client)
    return await querier.release_scheduler_lock(*args, **kwargs)

async def save_result_async(client, *args, **kwargs):
    querier = save_result_AsyncQuerier(client)
    return await querier.save_result(*args, **kwargs)

async def schedule_cron_job_runs_json_async(client, *args, **kwargs):
    querier = schedule_cron_job_runs_json_AsyncQuerier(client)
    return await querier.schedule_cron_job_runs_json(*args, **kwargs)

async def set_executor_to_lost_if_no_heartbeat_async(client, *args, **kwargs):
    querier = set_executor_to_lost_if_no_heartbeat_AsyncQuerier(client)
    return await querier.set_executor_to_lost_if_no_heartbeat(*args, **kwargs)

async def set_log_link_async(client, *args, **kwargs):
    querier = set_log_link_AsyncQuerier(client)
    return await querier.set_log_link(*args, **kwargs)

async def set_orphaned_task_execution_to_lost_and_retry_async(client, *args, **kwargs):
    querier = set_orphaned_task_execution_to_lost_and_retry_AsyncQuerier(client)
    return await querier.set_orphaned_task_execution_to_lost_and_retry(*args, **kwargs)

async def set_workflow_run_status_based_on_task_runs_async(client, *args, **kwargs):
    querier = set_workflow_run_status_based_on_task_runs_AsyncQuerier(client)
    return await querier.set_workflow_run_status_based_on_task_runs(*args, **kwargs)

async def skip_waiting_task_for_workflow_run_id_async(client, *args, **kwargs):
    querier = skip_waiting_task_for_workflow_run_id_AsyncQuerier(client)
    return await querier.skip_waiting_task_for_workflow_run_id(*args, **kwargs)

async def transition_task_state_async(client, *args, **kwargs):
    querier = transition_task_state_AsyncQuerier(client)
    return await querier.transition_task_state(*args, **kwargs)

async def trigger_execute_queued_cron_job_async(client, *args, **kwargs):
    querier = trigger_execute_queued_cron_job_AsyncQuerier(client)
    return await querier.trigger_execute_queued_cron_job(*args, **kwargs)

async def trim_task_stats_async(client, *args, **kwargs):
    querier = trim_task_stats_AsyncQuerier(client)
    return await querier.trim_task_stats(*args, **kwargs)

async def turn_off_cron_for_task_async(client, *args, **kwargs):
    querier = turn_off_cron_for_task_AsyncQuerier(client)
    return await querier.turn_off_cron_for_task(*args, **kwargs)

async def update_cron_job_confirmation_ts_async(client, *args, **kwargs):
    querier = update_cron_job_confirmation_ts_AsyncQuerier(client)
    return await querier.update_cron_job_confirmation_ts(*args, **kwargs)

async def update_executor_stats_async(client, *args, **kwargs):
    querier = update_executor_stats_AsyncQuerier(client)
    return await querier.update_executor_stats(*args, **kwargs)

async def update_hype_cron_job_confirmed_until_async(client, *args, **kwargs):
    querier = update_hype_cron_job_confirmed_until_AsyncQuerier(client)
    return await querier.update_hype_cron_job_confirmed_until(*args, **kwargs)

async def update_hype_cron_job_run_status_async(client, *args, **kwargs):
    querier = update_hype_cron_job_run_status_AsyncQuerier(client)
    return await querier.update_hype_cron_job_run_status(*args, **kwargs)

async def update_queues_on_executor_async(client, *args, **kwargs):
    querier = update_queues_on_executor_AsyncQuerier(client)
    return await querier.update_queues_on_executor(*args, **kwargs)

async def update_workflow_run_status_async(client, *args, **kwargs):
    querier = update_workflow_run_status_AsyncQuerier(client)
    return await querier.update_workflow_run_status(*args, **kwargs)


# Schema creation helpers
from typing import Protocol, Union

class DatabaseClient(Protocol):
    """Protocol for async database client that can execute queries"""
    async def execute(self, query, *args) -> None: ...

class SyncDatabaseClient(Protocol):
    """Protocol for sync database client that can execute queries"""
    def execute(self, query, *args) -> None: ...


def create_enums_sync(client: SyncDatabaseClient) -> None:
    """Create all database enum types (synchronous)"""
    create_cron_job_status_enum_sync(client)
    create_executor_status_enum_sync(client)
    create_job_source_type_enum_sync(client)
    create_task_run_status_enum_sync(client)
    create_workflow_run_status_enum_sync(client)

def create_tables_sync(client: SyncDatabaseClient) -> None:
    """Create all database tables and indexes (synchronous)"""
    create_executor_table_sync(client)
    create_app_table_sync(client)
    create_cron_job_table_sync(client)
    create_scheduler_lock_table_sync(client)
    create_stats_task_status_counts_table_sync(client)
    create_stats_task_status_counts_table_indexes_sync(client)
    create_task_def_table_sync(client)
    create_workflow_table_sync(client)
    create_system_log_table_sync(client)
    create_workflow_run_table_sync(client)
    create_cron_job_run_details_table_sync(client)
    create_task_run_table_sync(client)
    create_task_run_table_indexes_sync(client)
    create_results_table_sync(client)
    create_hyrex_kv_table_sync(client)

def create_functions_sync(client: SyncDatabaseClient) -> None:
    """Create all database functions and triggers (synchronous)"""
    create_advance_workflow_run_function_sync(client)
    create_conditionally_retry_task_func_sync(client)
    create_execute_queued_cron_job_function_sync(client)
    create_schedule_cron_job_runs_func_sync(client)
    create_set_workflow_run_status_based_on_task_runs_function_sync(client)
    create_task_run_function_sync(client)
    create_transition_task_run_state_func_sync(client)
    create_uuid7_function_sync(client)
    create_workflow_trigger_sync(client)

def create_hype_enums_sync(client: SyncDatabaseClient) -> None:
    """Create all hype-specific enum types (synchronous)"""
    create_hype_command_type_enum_sync(client)
    create_hype_cron_job_status_enum_sync(client)

def create_hype_tables_sync(client: SyncDatabaseClient) -> None:
    """Create all hype-specific tables (synchronous)"""
    create_hype_cron_job_table_sync(client)
    create_hype_cron_job_run_details_table_sync(client)

async def create_enums_async(client: DatabaseClient) -> None:
    """Create all database enum types (asynchronous)"""
    await create_cron_job_status_enum_async(client)
    await create_executor_status_enum_async(client)
    await create_job_source_type_enum_async(client)
    await create_task_run_status_enum_async(client)
    await create_workflow_run_status_enum_async(client)

async def create_tables_async(client: DatabaseClient) -> None:
    """Create all database tables and indexes (asynchronous)"""
    await create_executor_table_async(client)
    await create_app_table_async(client)
    await create_cron_job_table_async(client)
    await create_scheduler_lock_table_async(client)
    await create_stats_task_status_counts_table_async(client)
    await create_stats_task_status_counts_table_indexes_async(client)
    await create_task_def_table_async(client)
    await create_workflow_table_async(client)
    await create_system_log_table_async(client)
    await create_workflow_run_table_async(client)
    await create_cron_job_run_details_table_async(client)
    await create_task_run_table_async(client)
    await create_task_run_table_indexes_async(client)
    await create_results_table_async(client)
    await create_hyrex_kv_table_async(client)

async def create_functions_async(client: DatabaseClient) -> None:
    """Create all database functions and triggers (asynchronous)"""
    await create_advance_workflow_run_function_async(client)
    await create_conditionally_retry_task_func_async(client)
    await create_execute_queued_cron_job_function_async(client)
    await create_schedule_cron_job_runs_func_async(client)
    await create_set_workflow_run_status_based_on_task_runs_function_async(client)
    await create_task_run_function_async(client)
    await create_transition_task_run_state_func_async(client)
    await create_uuid7_function_async(client)
    await create_workflow_trigger_async(client)

async def create_hype_enums_async(client: DatabaseClient) -> None:
    """Create all hype-specific enum types (asynchronous)"""
    await create_hype_command_type_enum_async(client)
    await create_hype_cron_job_status_enum_async(client)

async def create_hype_tables_async(client: DatabaseClient) -> None:
    """Create all hype-specific tables (asynchronous)"""
    await create_hype_cron_job_table_async(client)
    await create_hype_cron_job_run_details_table_async(client)

# Define what's exported when using 'from sdkPostgresDispatcher import *'
__all__ = [
    # Models
    'models',
    # Schema helpers (sync)
    'create_enums_sync',
    'create_tables_sync',
    'create_functions_sync',
    'create_hype_enums_sync',
    'create_hype_tables_sync',
    # Schema helpers (async)
    'create_enums_async',
    'create_tables_async',
    'create_functions_async',
    'create_hype_enums_async',
    'create_hype_tables_async',
    # Query functions
    'acquire_scheduler_lock_async',
    'acquire_scheduler_lock_sync',
    'advance_workflow_run_func_async',
    'advance_workflow_run_func_sync',
    'batch_update_heartbeat_log_async',
    'batch_update_heartbeat_log_sync',
    'batch_update_heartbeat_on_executors_async',
    'batch_update_heartbeat_on_executors_sync',
    'claim_queued_hype_cron_job_runs_async',
    'claim_queued_hype_cron_job_runs_sync',
    'conditionally_retry_task_async',
    'conditionally_retry_task_sync',
    'count_queued_hype_cron_job_runs_async',
    'count_queued_hype_cron_job_runs_sync',
    'create_advance_workflow_run_func_async',
    'create_advance_workflow_run_func_sync',
    'create_conditionally_retry_task_func_async',
    'create_conditionally_retry_task_func_sync',
    'create_cron_job_for_sql_query_async',
    'create_cron_job_for_sql_query_sync',
    'create_cron_job_for_task_async',
    'create_cron_job_for_task_sync',
    'create_cron_job_status_enum_async',
    'create_cron_job_status_enum_sync',
    'create_execute_queued_cron_job_func_async',
    'create_execute_queued_cron_job_func_sync',
    'create_executor_status_enum_async',
    'create_executor_status_enum_sync',
    'create_executor_table_async',
    'create_executor_table_sync',
    'create_hype_command_type_enum_async',
    'create_hype_command_type_enum_sync',
    'create_hype_cron_job_run_details_async',
    'create_hype_cron_job_run_details_sync',
    'create_hype_cron_job_run_details_table_async',
    'create_hype_cron_job_run_details_table_sync',
    'create_hype_cron_job_status_enum_async',
    'create_hype_cron_job_status_enum_sync',
    'create_hype_cron_job_table_async',
    'create_hype_cron_job_table_sync',
    'create_hyrex_app_table_async',
    'create_hyrex_app_table_sync',
    'create_hyrex_cron_job_run_details_table_async',
    'create_hyrex_cron_job_run_details_table_sync',
    'create_hyrex_cron_job_table_async',
    'create_hyrex_cron_job_table_sync',
    'create_hyrex_kv_table_async',
    'create_hyrex_kv_table_sync',
    'create_hyrex_scheduler_lock_table_async',
    'create_hyrex_scheduler_lock_table_sync',
    'create_hyrex_stats_task_status_counts_table_async',
    'create_hyrex_stats_task_status_counts_table_indexes_async',
    'create_hyrex_stats_task_status_counts_table_indexes_sync',
    'create_hyrex_stats_task_status_counts_table_sync',
    'create_hyrex_task_def_table_async',
    'create_hyrex_task_def_table_sync',
    'create_hyrex_task_run_table_async',
    'create_hyrex_task_run_table_indexes_async',
    'create_hyrex_task_run_table_indexes_sync',
    'create_hyrex_task_run_table_sync',
    'create_job_source_type_enum_async',
    'create_job_source_type_enum_sync',
    'create_results_table_async',
    'create_results_table_sync',
    'create_schedule_cron_job_runs_func_async',
    'create_schedule_cron_job_runs_func_sync',
    'create_set_workflow_run_status_based_on_task_runs_func_async',
    'create_set_workflow_run_status_based_on_task_runs_func_sync',
    'create_system_log_table_async',
    'create_system_log_table_sync',
    'create_task_run_async',
    'create_task_run_status_enum_async',
    'create_task_run_status_enum_sync',
    'create_task_run_sync',
    'create_transition_task_run_state_func_async',
    'create_transition_task_run_state_func_sync',
    'create_uuid7_func_async',
    'create_uuid7_func_sync',
    'create_workflow_run_async',
    'create_workflow_run_status_enum_async',
    'create_workflow_run_status_enum_sync',
    'create_workflow_run_sync',
    'create_workflow_run_table_async',
    'create_workflow_run_table_sync',
    'create_workflow_table_async',
    'create_workflow_table_sync',
    'create_workflow_trigger_func_async',
    'create_workflow_trigger_func_sync',
    'disconnect_executor_async',
    'disconnect_executor_sync',
    'fetch_active_queue_names_async',
    'fetch_active_queue_names_sync',
    'fetch_result_async',
    'fetch_result_sync',
    'fetch_task_async',
    'fetch_task_sync',
    'fetch_task_with_concurrency_limit_async',
    'fetch_task_with_concurrency_limit_sync',
    'fill_historical_task_status_counts_table_async',
    'fill_historical_task_status_counts_table_sync',
    'get_active_hype_cron_jobs_async',
    'get_active_hype_cron_jobs_sync',
    'get_all_task_defs_async',
    'get_all_task_defs_sync',
    'get_app_name_async',
    'get_app_name_sync',
    'get_cron_job_run_details_async',
    'get_cron_job_run_details_sync',
    'get_cron_jobs_paginated_async',
    'get_cron_jobs_paginated_sync',
    'get_executor_by_id_async',
    'get_executor_by_id_sync',
    'get_executors_paginated_async',
    'get_executors_paginated_sync',
    'get_hype_cron_job_by_name_async',
    'get_hype_cron_job_by_name_sync',
    'get_project_stats_async',
    'get_project_stats_sync',
    'get_task_attempts_by_durable_id_async',
    'get_task_attempts_by_durable_id_sync',
    'get_task_by_name_async',
    'get_task_by_name_sync',
    'get_task_def_async',
    'get_task_def_sync',
    'get_task_run_by_id_async',
    'get_task_run_by_id_sync',
    'get_task_runs_by_status_paginated_async',
    'get_task_runs_by_status_paginated_sync',
    'get_task_runs_paginated_async',
    'get_task_runs_paginated_sync',
    'get_tasks_paginated_async',
    'get_tasks_paginated_sync',
    'get_workflow_by_name_async',
    'get_workflow_by_name_sync',
    'get_workflow_run_by_id_async',
    'get_workflow_run_by_id_sync',
    'get_workflow_run_task_runs_async',
    'get_workflow_run_task_runs_sync',
    'get_workflow_runs_paginated_async',
    'get_workflow_runs_paginated_sync',
    'get_workflows_paginated_async',
    'get_workflows_paginated_sync',
    'insert_single_task_status_counts_row_async',
    'insert_single_task_status_counts_row_sync',
    'insert_workflow_run_async',
    'insert_workflow_run_sync',
    'kv_delete_value_async',
    'kv_delete_value_sync',
    'kv_flush_keys_async',
    'kv_flush_keys_sync',
    'kv_get_value_async',
    'kv_get_value_sync',
    'kv_list_keys_paginated_async',
    'kv_list_keys_paginated_sync',
    'kv_set_value_async',
    'kv_set_value_sync',
    'mark_lost_executors_async',
    'mark_lost_executors_sync',
    'pull_active_cron_expressions_async',
    'pull_active_cron_expressions_sync',
    'register_app_info_async',
    'register_app_info_sync',
    'register_executor_async',
    'register_executor_sync',
    'register_hype_cron_job_async',
    'register_hype_cron_job_sync',
    'register_task_def_async',
    'register_task_def_sync',
    'register_workflow_async',
    'register_workflow_sync',
    'release_scheduler_lock_async',
    'release_scheduler_lock_sync',
    'save_result_async',
    'save_result_sync',
    'schedule_cron_job_runs_json_async',
    'schedule_cron_job_runs_json_sync',
    'set_executor_to_lost_if_no_heartbeat_async',
    'set_executor_to_lost_if_no_heartbeat_sync',
    'set_log_link_async',
    'set_log_link_sync',
    'set_orphaned_task_execution_to_lost_and_retry_async',
    'set_orphaned_task_execution_to_lost_and_retry_sync',
    'set_workflow_run_status_based_on_task_runs_async',
    'set_workflow_run_status_based_on_task_runs_sync',
    'skip_waiting_task_for_workflow_run_id_async',
    'skip_waiting_task_for_workflow_run_id_sync',
    'transition_task_state_async',
    'transition_task_state_sync',
    'trigger_execute_queued_cron_job_async',
    'trigger_execute_queued_cron_job_sync',
    'trim_task_stats_async',
    'trim_task_stats_sync',
    'turn_off_cron_for_task_async',
    'turn_off_cron_for_task_sync',
    'update_cron_job_confirmation_ts_async',
    'update_cron_job_confirmation_ts_sync',
    'update_executor_stats_async',
    'update_executor_stats_sync',
    'update_hype_cron_job_confirmed_until_async',
    'update_hype_cron_job_confirmed_until_sync',
    'update_hype_cron_job_run_status_async',
    'update_hype_cron_job_run_status_sync',
    'update_queues_on_executor_async',
    'update_queues_on_executor_sync',
    'update_workflow_run_status_async',
    'update_workflow_run_status_sync',
]
