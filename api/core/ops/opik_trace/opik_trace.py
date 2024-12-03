import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import opik
from opik.api_objects import opik_client
from opik.api_objects.helpers import generate_id
from opik.api_objects.span import SpanData
from opik.api_objects.trace import TraceData
from opik.url_helpers import get_project_url

from core.ops.base_trace_instance import BaseTraceInstance
from core.ops.entities.config_entity import OpikConfig
from core.ops.entities.trace_entity import (
    BaseTraceInfo,
    DatasetRetrievalTraceInfo,
    GenerateNameTraceInfo,
    MessageTraceInfo,
    ModerationTraceInfo,
    SuggestedQuestionTraceInfo,
    ToolTraceInfo,
    TraceTaskName,
    WorkflowTraceInfo,
)
from core.ops.utils import filter_none_values
from extensions.ext_database import db
from models.model import EndUser
from models.workflow import WorkflowNodeExecution

logger = logging.getLogger(__name__)

from collections.abc import MutableMapping


class DictPersistJSON(MutableMapping):
    def __init__(self, filename):
        self.filename = filename
        self._data = {}
        self._load()

    def _load(self):
        if os.path.isfile(self.filename) and os.path.getsize(self.filename) > 0:
            with open(self.filename) as fh:
                self._data = json.load(fh)

    def _dump(self):
        with open(self.filename, "w") as fh:
            json.dump(self._data, fh)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, val):
        self._data[key] = val
        self._dump()

    def __delitem__(self, key):
        del self._data[key]
        self._dump()

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


def wrap_dict(key_name, data):
    if not isinstance(data, dict):
        return {key_name: data}

    return data


class OpikDataTrace(BaseTraceInstance):
    def __init__(
        self,
        opik_config: OpikConfig,
    ):
        super().__init__(opik_config)
        print("Opik config", opik_config)
        # TODO: Remove call to configure once API Key is supported in Opik.__init__, CREATE TICKET
        opik.configure(api_key=opik_config.api_key, url=opik_config.url, use_local=False)
        self.opik_client = opik_client.Opik(
            _use_batching=True, project_name=opik_config.project, workspace=opik_config.workspace, host=opik_config.url
        )
        self.project = opik_config.project
        # self.langfuse_client = Langfuse(
        #     public_key=langfuse_config.public_key,
        #     secret_key=langfuse_config.secret_key,
        #     host=langfuse_config.host,
        # )
        self.file_base_url = os.getenv("FILES_URL", "http://127.0.0.1:5001")
        self.uuid_mapping = DictPersistJSON("/tmp/dify.json")
        # self.uuid_mapping = defaultdict(generate_id)  # TODO: Temporary

    def trace(self, trace_info: BaseTraceInfo):
        print("TRACE!", type(trace_info))
        if isinstance(trace_info, WorkflowTraceInfo):
            self.workflow_trace(trace_info)
        if isinstance(trace_info, MessageTraceInfo):
            self.message_trace(trace_info)
        if isinstance(trace_info, ModerationTraceInfo):
            self.moderation_trace(trace_info)
        if isinstance(trace_info, SuggestedQuestionTraceInfo):
            self.suggested_question_trace(trace_info)
        if isinstance(trace_info, DatasetRetrievalTraceInfo):
            self.dataset_retrieval_trace(trace_info)
        if isinstance(trace_info, ToolTraceInfo):
            self.tool_trace(trace_info)
        if isinstance(trace_info, GenerateNameTraceInfo):
            self.generate_name_trace(trace_info)

    def workflow_trace(self, trace_info: WorkflowTraceInfo):
        trace_id = trace_info.message_id or trace_info.workflow_app_log_id or trace_info.workflow_run_id

        if trace_info.message_id:
            # TODO: Create dict directly instead
            trace_data = TraceData(
                id=self.uuid_mapping.setdefault(trace_info.message_id, generate_id()),
                name=TraceTaskName.MESSAGE_TRACE.value,
                start_time=trace_info.start_time,
                end_time=trace_info.end_time,
                metadata=trace_info.metadata,
                input=wrap_dict("input", trace_info.workflow_run_inputs),
                output=wrap_dict("output", trace_info.workflow_run_outputs),
                tags=["message", "workflow"],
                project_name=self.project,
            )
            trace_id = trace_data.id  # TODO: TEmporary
            self.add_trace(trace_data)

        span_id = trace_info.workflow_app_log_id or trace_info.workflow_run_id
        span_data = SpanData(
            trace_id=trace_id,
            id=self.uuid_mapping.setdefault(span_id, generate_id()),
            parent_span_id=None,
            name=TraceTaskName.WORKFLOW_TRACE.value,
            type="tool",
            start_time=trace_info.workflow_data.created_at,
            end_time=trace_info.workflow_data.finished_at,
            metadata=trace_info.metadata,
            input=wrap_dict("input", trace_info.workflow_run_inputs),
            output=wrap_dict("output", trace_info.workflow_run_outputs),
            tags=["workflow"],
            project_name=self.project,
        )
        # langsmith_run = LangSmithRunModel(
        #     file_list=trace_info.file_list,
        #     total_tokens=trace_info.total_tokens,
        #     id=trace_info.workflow_app_log_id or trace_info.workflow_run_id,
        #     name=TraceTaskName.WORKFLOW_TRACE.value,
        #     inputs=trace_info.workflow_run_inputs,
        #     run_type=LangSmithRunType.tool,
        #     start_time=trace_info.workflow_data.created_at,
        #     end_time=trace_info.workflow_data.finished_at,
        #     outputs=trace_info.workflow_run_outputs,
        #     extra={
        #         "metadata": trace_info.metadata,
        #     },
        #     error=trace_info.error,
        #     tags=["workflow"],
        #     parent_run_id=trace_info.message_id or None,
        #     trace_id=trace_id,
        #     dotted_order=workflow_dotted_order,
        # )

        self.add_span(span_data)

        # through workflow_run_id get all_nodes_execution
        workflow_nodes_execution_id_records = (
            db.session.query(WorkflowNodeExecution.id)
            .filter(WorkflowNodeExecution.workflow_run_id == trace_info.workflow_run_id)
            .all()
        )

        for node_execution_id_record in workflow_nodes_execution_id_records:
            node_execution = (
                db.session.query(
                    WorkflowNodeExecution.id,
                    WorkflowNodeExecution.tenant_id,
                    WorkflowNodeExecution.app_id,
                    WorkflowNodeExecution.title,
                    WorkflowNodeExecution.node_type,
                    WorkflowNodeExecution.status,
                    WorkflowNodeExecution.inputs,
                    WorkflowNodeExecution.outputs,
                    WorkflowNodeExecution.created_at,
                    WorkflowNodeExecution.elapsed_time,
                    WorkflowNodeExecution.process_data,
                    WorkflowNodeExecution.execution_metadata,
                )
                .filter(WorkflowNodeExecution.id == node_execution_id_record.id)
                .first()
            )

            if not node_execution:
                continue

            node_execution_id = node_execution.id
            tenant_id = node_execution.tenant_id
            app_id = node_execution.app_id
            node_name = node_execution.title
            node_type = node_execution.node_type
            status = node_execution.status
            if node_type == "llm":
                inputs = (
                    json.loads(node_execution.process_data).get("prompts", {}) if node_execution.process_data else {}
                )
            else:
                inputs = json.loads(node_execution.inputs) if node_execution.inputs else {}
            outputs = json.loads(node_execution.outputs) if node_execution.outputs else {}
            created_at = node_execution.created_at or datetime.now()
            elapsed_time = node_execution.elapsed_time
            finished_at = created_at + timedelta(seconds=elapsed_time)

            execution_metadata = (
                json.loads(node_execution.execution_metadata) if node_execution.execution_metadata else {}
            )
            node_total_tokens = execution_metadata.get("total_tokens", 0)
            metadata = execution_metadata.copy()
            metadata.update(
                {
                    "workflow_run_id": trace_info.workflow_run_id,
                    "node_execution_id": node_execution_id,
                    "tenant_id": tenant_id,
                    "app_id": app_id,
                    "app_name": node_name,
                    "node_type": node_type,
                    "status": status,
                }
            )

            process_data = json.loads(node_execution.process_data) if node_execution.process_data else {}
            if process_data and process_data.get("model_mode") == "chat":
                run_type = "llm"
                metadata.update(
                    {
                        "ls_provider": process_data.get("model_provider", ""),
                        "ls_model_name": process_data.get("model_name", ""),
                    }
                )
            else:
                run_type = "tool"

            parent_span_id = trace_info.workflow_app_log_id or trace_info.workflow_run_id
            span_data = SpanData(
                trace_id=trace_id,
                id=node_execution_id,
                parent_span_id=self.uuid_mapping.setdefault(parent_span_id, generate_id()),
                name=node_type,
                type=run_type,
                start_time=created_at,
                end_time=finished_at,
                metadata=metadata,
                input=wrap_dict("input", inputs),
                output=wrap_dict("output", outputs),
                tags=["node_execution"],
                project_name=self.project,
            )

            print("SPAN", span_data)
            self.add_span(span_data)

    def message_trace(self, trace_info: MessageTraceInfo):
        print("MEssage trace", id(self), "=" * 80)
        # import pprint

        # pprint.pprint(trace_info.__dict__)

        # get message file data
        file_list = trace_info.file_list
        message_file_data: MessageFile = trace_info.message_file_data
        file_url = f"{self.file_base_url}/{message_file_data.url}" if message_file_data else ""
        file_list.append(file_url)
        metadata = trace_info.metadata
        message_data = trace_info.message_data
        print("Message data", repr(message_data), type(message_data))
        message_id = trace_info.message_id

        user_id = message_data.from_account_id
        metadata["user_id"] = user_id

        if message_data.from_end_user_id:
            end_user_data: EndUser = (
                db.session.query(EndUser).filter(EndUser.id == message_data.from_end_user_id).first()
            )
            if end_user_data is not None:
                end_user_id = end_user_data.session_id
                metadata["end_user_id"] = end_user_id

        trace_data = TraceData(
            id=self.uuid_mapping.setdefault(message_id, generate_id()),
            name=TraceTaskName.MESSAGE_TRACE.value,
            start_time=trace_info.start_time,
            end_time=trace_info.end_time,
            metadata=metadata,
            input=trace_info.inputs,
            output=message_data.answer,
            tags=["message", str(trace_info.conversation_mode)],
            project_name=self.project,
        )
        print("Message trace", trace_data.id)
        self.add_trace(trace_data)

        span_data = SpanData(
            trace_id=trace_data.id,
            name="llm",
            type="llm",
            start_time=trace_info.start_time,
            end_time=trace_info.end_time,
            metadata=metadata,
            input={"input": trace_info.inputs},
            output={"output": message_data.answer},
            tags=["llm", str(trace_info.conversation_mode)],
            usage={
                "completion_tokens": trace_info.answer_tokens,
                "prompt_tokens": trace_info.message_tokens,
                "total_tokens": trace_info.total_tokens,
            },
            project_name=self.project,
        )
        self.add_span(span_data)

    def moderation_trace(self, trace_info: ModerationTraceInfo):
        langsmith_run = LangSmithRunModel(
            name=TraceTaskName.MODERATION_TRACE.value,
            inputs=trace_info.inputs,
            outputs={
                "action": trace_info.action,
                "flagged": trace_info.flagged,
                "preset_response": trace_info.preset_response,
                "inputs": trace_info.inputs,
            },
            run_type=LangSmithRunType.tool,
            extra={
                "metadata": trace_info.metadata,
            },
            tags=["moderation"],
            parent_run_id=trace_info.message_id,
            start_time=trace_info.start_time or trace_info.message_data.created_at,
            end_time=trace_info.end_time or trace_info.message_data.updated_at,
        )

        self.add_run(langsmith_run)

    def suggested_question_trace(self, trace_info: SuggestedQuestionTraceInfo):
        message_data = trace_info.message_data
        suggested_question_run = LangSmithRunModel(
            name=TraceTaskName.SUGGESTED_QUESTION_TRACE.value,
            inputs=trace_info.inputs,
            outputs=trace_info.suggested_question,
            run_type=LangSmithRunType.tool,
            extra={
                "metadata": trace_info.metadata,
            },
            tags=["suggested_question"],
            parent_run_id=trace_info.message_id,
            start_time=trace_info.start_time or message_data.created_at,
            end_time=trace_info.end_time or message_data.updated_at,
        )

        self.add_run(suggested_question_run)

    def dataset_retrieval_trace(self, trace_info: DatasetRetrievalTraceInfo):
        dataset_retrieval_run = LangSmithRunModel(
            name=TraceTaskName.DATASET_RETRIEVAL_TRACE.value,
            inputs=trace_info.inputs,
            outputs={"documents": trace_info.documents},
            run_type=LangSmithRunType.retriever,
            extra={
                "metadata": trace_info.metadata,
            },
            tags=["dataset_retrieval"],
            parent_run_id=trace_info.message_id,
            start_time=trace_info.start_time or trace_info.message_data.created_at,
            end_time=trace_info.end_time or trace_info.message_data.updated_at,
        )

        self.add_run(dataset_retrieval_run)

    def tool_trace(self, trace_info: ToolTraceInfo):
        # print("tool_trace", id(self), "=" * 80)
        # import pprint

        # pprint.pprint(trace_info.__dict__)

        span_data = SpanData(
            trace_id=self.uuid_mapping.setdefault(trace_info.message_id, generate_id()),
            name=trace_info.tool_name,
            type="tool",
            start_time=trace_info.start_time,
            end_time=trace_info.end_time,
            metadata=trace_info.metadata,
            input=wrap_dict("input", trace_info.tool_inputs),
            output=wrap_dict("output", trace_info.tool_outputs),
            tags=["tool", trace_info.tool_name],
        )
        print("tool trace id", span_data.input)
        print("tool trace id", span_data.output)

        self.add_span(span_data)

    def generate_name_trace(self, trace_info: GenerateNameTraceInfo):
        name_run = LangSmithRunModel(
            name=TraceTaskName.GENERATE_NAME_TRACE.value,
            inputs=trace_info.inputs,
            outputs=trace_info.outputs,
            run_type=LangSmithRunType.tool,
            extra={
                "metadata": trace_info.metadata,
            },
            tags=["generate_name"],
            start_time=trace_info.start_time or datetime.now(),
            end_time=trace_info.end_time or datetime.now(),
        )

        self.add_run(name_run)

    def add_trace(self, opik_trace_data: Optional[TraceData] = None):
        try:
            self.opik_client.trace(**opik_trace_data.__dict__)
            logger.debug("Opik Trace created successfully")
        except Exception as e:
            raise ValueError(f"Opik Failed to create trace: {str(e)}")

    def add_span(self, opik_span_data: Optional[SpanData] = None):
        try:
            self.opik_client.span(**opik_span_data.__dict__)
            logger.debug("Opik Span created successfully")
        except Exception as e:
            raise ValueError(f"Opik Failed to create span: {str(e)}")

    def update_span(self, span, opik_span_data: Optional[SpanData] = None):
        format_span_data = filter_none_values(opik_span_data.model_dump()) if opik_span_data else {}

        span.end(**format_span_data)

    def api_check(self):
        try:
            # return self.opik_client.auth_check()
            return True
            # return self.langfuse_client.auth_check()
        except Exception as e:
            logger.debug(f"Opik API check failed: {str(e)}", exc_info=True)
            raise ValueError(f"Opik API check failed: {str(e)}")

    def get_project_url(self):
        try:
            return get_project_url(workspace=self.opik_client._workspace, project_name=self.project)
        except Exception as e:
            logger.debug(f"Opik get run url failed: {str(e)}", exc_info=True)
            raise ValueError(f"Opik get run url failed: {str(e)}")
