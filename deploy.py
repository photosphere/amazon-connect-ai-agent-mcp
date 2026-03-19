#!/usr/bin/env python3
"""
deploy.py - 创建AI Agent和Contact Flow到Amazon Connect实例

用法:
    # 使用默认配置
    python deploy.py

    # key=value 格式（原有）
    python deploy.py assistant_id=f8d5155e instance_id=b7e4b4ed region=us-east-1

    # --key value 格式（新增支持）
    python deploy.py --assistant-id f8d5155e --instance-id b7e4b4ed --region us-east-1

    # --key=value 格式（新增支持）
    python deploy.py --assistant-id=f8d5155e --instance-id=b7e4b4ed

支持的命令行参数:
    assistant_id  / --assistant-id    Amazon Q in Connect Assistant ID
    instance_id   / --instance-id     Amazon Connect 实例ID
    region        / --region          AWS区域
    ai_agent_name / --ai-agent-name   AI Agent名称
    flow_name     / --flow-name       Contact Flow名称
    ai_agent_json / --ai-agent-json   ai_agent.json文件路径
    flow_json     / --flow-json       connect_flow_mcp.json文件路径
    output_file   / --output-file     结果输出文件路径
"""

import boto3
import json
import copy
import uuid
import logging
import sys
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# ============================================================
# 默认配置（可通过命令行参数覆盖）
# ============================================================
DEFAULT_CONFIG = {
    'assistant_id':    'f8d5155e-7514-42f9-aca8-5aa224722e8a',
    'instance_id':     'b7e4b4ed-1bdf-4b14-b624-d9328f08725a',
    'region':          'us-east-1',
    'ai_agent_name':   'SelfServiceOrchestrator_Query',
    'flow_name':       'MCP Inbound Flow',
    'ai_agent_json':   'ai_agent.json',
    'flow_json':       'connect_flow_mcp.json',
    'output_file':     None,
}

# ============================================================
# 日志配置
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================
# 命令行参数解析（支持 key=value / --key=value / --key value）
# ============================================================

def parse_args(argv: List[str]) -> Dict[str, str]:
    """
    解析命令行参数，支持以下三种格式:
      1. key=value                  （原有格式）
      2. --key=value                （GNU 长选项 = 格式）
      3. --key value                （GNU 长选项 空格分隔格式）

    参数名中的连字符 '-' 自动转为下划线 '_'，例如:
      --assistant-id xxx  →  assistant_id = xxx

    Args:
        argv: sys.argv[1:]

    Returns:
        解析后的参数字典
    """
    parsed         = {}
    supported_keys = set(DEFAULT_CONFIG.keys())
    i              = 0

    while i < len(argv):
        arg = argv[i]

        # ── 格式1 & 2: key=value 或 --key=value ──────────────────
        if '=' in arg:
            raw_key, _, value = arg.partition('=')
            # 去掉前导 '--' 或 '-'，连字符转下划线
            key = raw_key.lstrip('-').replace('-', '_').strip()

            if not key:
                logger.warning(f"忽略无效参数（键为空）: {arg}")
                i += 1
                continue

            if key not in supported_keys:
                logger.warning(
                    f"忽略未知参数: {key}  "
                    f"（支持的参数: {', '.join(sorted(supported_keys))}）"
                )
                i += 1
                continue

            parsed[key] = value.strip()
            logger.info(f"命令行覆盖参数: {key} = {value.strip()}")
            i += 1

        # ── 格式3: --key value（下一个 token 是值）───────────────
        elif arg.startswith('-'):
            raw_key = arg.lstrip('-').replace('-', '_').strip()

            if not raw_key:
                logger.warning(f"忽略无效参数（仅有短横线）: {arg}")
                i += 1
                continue

            # 判断下一个 token 是否是值（非选项标记）
            if i + 1 < len(argv) and not argv[i + 1].startswith('-'):
                value = argv[i + 1].strip()
                i += 2
            else:
                # 布尔标志位（本项目暂不需要，但做防御处理）
                value = 'true'
                i += 1

            if raw_key not in supported_keys:
                logger.warning(
                    f"忽略未知参数: {raw_key}  "
                    f"（支持的参数: {', '.join(sorted(supported_keys))}）"
                )
                continue

            parsed[raw_key] = value
            logger.info(f"命令行覆盖参数: {raw_key} = {value}")

        # ── 无法识别的裸词 ─────────────────────────────────────────
        else:
            logger.warning(
                f"忽略无效参数（不含 '=' 且无前导 '--'）: {arg}"
            )
            i += 1

    return parsed


def build_config(argv: List[str]) -> Dict:
    """
    合并默认配置和命令行参数，返回最终运行配置

    Args:
        argv: sys.argv[1:]

    Returns:
        最终配置字典
    """
    config    = dict(DEFAULT_CONFIG)   # 复制默认配置
    overrides = parse_args(argv)       # 解析命令行
    config.update(overrides)           # 命令行覆盖默认值

    # output_file 若为字符串 'None' 则转为 None
    if config.get('output_file') in ('None', 'none', ''):
        config['output_file'] = None

    return config


# ============================================================
# 工具函数
# ============================================================

def load_json_file(file_path: str) -> Dict:
    """加载JSON文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"文件不存在: {file_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON格式错误: {file_path} → {e}")


def save_json_file(data: Dict, filename: Optional[str] = None) -> str:
    """保存结果到JSON文件，filename为None时自动生成文件名"""
    if filename is None:
        ts       = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'deploy_result_{ts}.json'

    def _serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"不支持序列化的类型: {type(obj)}")

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(
            {'timestamp': datetime.now().isoformat(), 'data': data},
            f, ensure_ascii=False, indent=2, default=_serializer
        )

    logger.info(f"结果已保存: {filename}")
    return filename


def get_account_id(region: Optional[str] = None) -> str:
    """获取当前AWS账号ID"""
    client = boto3.client('sts', region_name=region) if region \
        else boto3.client('sts')
    return client.get_caller_identity()['Account']


def make_connect_client(region: Optional[str] = None):
    """创建 connect boto3 客户端"""
    return boto3.client('connect', region_name=region) if region \
        else boto3.client('connect')


def make_qconnect_client(region: Optional[str] = None):
    """创建 qconnect boto3 客户端"""
    return boto3.client('qconnect', region_name=region) if region \
        else boto3.client('qconnect')


# ============================================================
# Connect Queue 查询
# ============================================================

def list_all_queues(instance_id: str, region: Optional[str] = None) -> List[Dict]:
    """列出Connect实例下所有Queue（自动翻页）"""
    client    = make_connect_client(region)
    queues    = []
    paginator = client.get_paginator('list_queues')

    for page in paginator.paginate(
        InstanceId=instance_id,
        QueueTypes=['STANDARD', 'AGENT']
    ):
        queues.extend(page.get('QueueSummaryList', []))

    logger.info(f"实例 {instance_id} 共找到 {len(queues)} 个Queue")

    if queues:
        logger.debug(f"Queue字段示例: {json.dumps(queues[0], default=str)}")

    return queues


def build_queue_name_to_arn_map(
    instance_id: str,
    region: Optional[str] = None
) -> Dict[str, str]:
    """构建 Queue名称 → Queue ARN 映射表"""
    queues  = list_all_queues(instance_id, region)
    mapping = {}
    skipped = 0

    for q in queues:
        name       = q.get('Name') or q.get('name') or ''
        arn        = q.get('Arn')  or q.get('arn')  or ''
        queue_type = q.get('QueueType') or q.get('queueType') or ''

        if not name:
            if queue_type != 'AGENT':
                logger.warning(
                    f"  跳过无Name的Queue [type={queue_type}]: "
                    f"{json.dumps(q, default=str)}"
                )
            skipped += 1
            continue

        if not arn:
            logger.warning(f"  跳过无ARN的Queue: name={name}")
            skipped += 1
            continue

        mapping[name] = arn

    logger.info(f"Queue映射表: {len(mapping)} 条有效, {skipped} 条跳过")
    for name, arn in mapping.items():
        logger.info(f"  [{name}] → {arn}")

    return mapping


# ============================================================
# AI Agent 创建
# ============================================================

def load_ai_agent_config(json_file: str) -> Dict:
    """从 ai_agent.json 加载 aiAgent 配置段"""
    data     = load_json_file(json_file)
    ai_agent = data.get('data', {}).get('aiAgent', {})

    if not ai_agent:
        raise ValueError(
            f"'{json_file}' 中未找到 data.aiAgent，请检查文件格式"
        )

    logger.info(f"加载AI Agent配置: {json_file}")
    logger.info(f"  原始名称: {ai_agent.get('name')}")
    logger.info(f"  类型:     {ai_agent.get('type')}")
    return ai_agent


def verify_assistant_exists(
    assistant_id: str,
    region: Optional[str] = None
) -> bool:
    """验证Assistant是否存在"""
    client = make_qconnect_client(region)
    try:
        client.get_assistant(assistantId=assistant_id)
        return True
    except client.exceptions.ResourceNotFoundException:
        return False
    except Exception as e:
        logger.warning(f"验证Assistant时出错: {e}")
        return False


def update_connect_instance_arn_in_config(
    configuration: Dict,
    instance_id: str,
    region: str,
    account_id: str
) -> Dict:
    """将 orchestrationAIAgentConfiguration.connectInstanceArn 替换为新实例ARN"""
    new_arn = (
        f"arn:aws:connect:{region}:{account_id}:instance/{instance_id}"
    )
    key = 'orchestrationAIAgentConfiguration'

    if key in configuration:
        old_arn = configuration[key].get('connectInstanceArn', 'N/A')
        configuration[key]['connectInstanceArn'] = new_arn
        logger.info(f"更新connectInstanceArn:")
        logger.info(f"  旧: {old_arn}")
        logger.info(f"  新: {new_arn}")

    return configuration


def validate_ai_agent_config(
    configuration: Dict,
    region: str
) -> Tuple[bool, str]:
    """验证 AI Agent configuration 是否有效"""
    key = 'orchestrationAIAgentConfiguration'

    if key not in configuration:
        return True, ""

    connect_arn = configuration[key].get('connectInstanceArn', '')
    if not connect_arn:
        return False, "配置中缺少 connectInstanceArn"

    try:
        parts       = connect_arn.split(':')
        arn_region  = parts[3]
        instance_id = parts[5].split('/')[-1]

        if arn_region != region:
            return False, (
                f"connectInstanceArn 区域({arn_region})"
                f" 与当前区域({region})不匹配"
            )

        make_connect_client(region).describe_instance(InstanceId=instance_id)
        return True, ""

    except Exception as e:
        err = str(e)
        if 'ResourceNotFoundException' in err:
            return False, f"Connect实例 '{instance_id}' 不存在于区域 '{region}'"
        return False, f"验证Connect实例时出错: {err}"


def create_ai_agent_api(
    assistant_id: str,
    name: str,
    agent_type: str,
    configuration: Dict,
    visibility_status: str = 'PUBLISHED',
    description: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    region: Optional[str] = None
) -> Dict:
    """调用 qconnect.create_ai_agent API"""
    client = make_qconnect_client(region)

    params = {
        'assistantId':      assistant_id,
        'name':             name,
        'type':             agent_type,
        'configuration':    configuration,
        'visibilityStatus': visibility_status,
        'clientToken':      str(uuid.uuid4()),
    }
    if description:
        params['description'] = description
    if tags:
        params['tags'] = tags

    logger.info(f"调用 create_ai_agent:")
    logger.info(f"  名称:   {name}")
    logger.info(f"  类型:   {agent_type}")
    logger.info(f"  可见性: {visibility_status}")

    response = client.create_ai_agent(**params)
    agent    = response.get('aiAgent', {})

    logger.info(f"✓ AI Agent创建成功")
    logger.info(f"  ID:  {agent.get('aiAgentId')}")
    logger.info(f"  ARN: {agent.get('aiAgentArn')}")
    return response


def create_ai_agent_from_json(
    json_file: str,
    assistant_id: str,
    ai_agent_name: str,
    instance_id: str,
    region: str,
    account_id: str
) -> Dict:
    """从 ai_agent.json 文件完整创建 AI Agent"""
    ai_agent_config = load_ai_agent_config(json_file)

    name              = ai_agent_name
    agent_type        = ai_agent_config.get('type')
    configuration     = copy.deepcopy(ai_agent_config.get('configuration', {}))
    visibility_status = ai_agent_config.get('visibilityStatus', 'PUBLISHED')
    description       = ai_agent_config.get('description')
    tags              = ai_agent_config.get('tags') or {}

    logger.info(f"配置摘要:")
    logger.info(f"  原始名称 → 新名称: {ai_agent_config.get('name')} → {name}")
    logger.info(f"  类型:     {agent_type}")
    logger.info(f"  Assistant ID: {assistant_id}")

    logger.info("验证 Assistant 是否存在...")
    if not verify_assistant_exists(assistant_id, region):
        raise ValueError(
            f"Assistant '{assistant_id}' 不存在于区域 '{region}'。\n"
            f"  请运行: aws qconnect list-assistants --region {region}"
        )
    logger.info(f"✓ Assistant 验证通过: {assistant_id}")

    logger.info(f"更新 connectInstanceArn → instance/{instance_id}")
    configuration = update_connect_instance_arn_in_config(
        configuration, instance_id, region, account_id
    )

    logger.info("验证 AI Agent 配置...")
    is_valid, error_msg = validate_ai_agent_config(configuration, region)
    if not is_valid:
        raise ValueError(f"AI Agent配置验证失败: {error_msg}")
    logger.info("✓ 配置验证通过")

    return create_ai_agent_api(
        assistant_id=assistant_id,
        name=name,
        agent_type=agent_type,
        configuration=configuration,
        visibility_status=visibility_status,
        description=description,
        tags=tags,
        region=region
    )


# ============================================================
# Flow 更新
# ============================================================

def extract_queue_refs(flow_content: Dict) -> List[Dict]:
    """提取Flow中所有 UpdateContactTargetQueue Action 的Queue引用"""
    refs = []

    for action in flow_content.get('Actions', []):
        if action.get('Type') != 'UpdateContactTargetQueue':
            continue

        action_id = action.get('Identifier', '')
        queue_arn = action.get('Parameters', {}).get('QueueId', '')

        if not queue_arn:
            continue

        display_name = (
            flow_content
            .get('Metadata', {})
            .get('ActionMetadata', {})
            .get(action_id, {})
            .get('parameters', {})
            .get('QueueId', {})
            .get('displayName', '')
        )

        refs.append({
            'action_id':    action_id,
            'queue_arn':    queue_arn,
            'display_name': display_name,
        })
        logger.info(
            f"  发现Queue引用: "
            f"action={action_id}, displayName='{display_name}', arn={queue_arn}"
        )

    return refs


def resolve_queue_arn(
    old_arn: str,
    display_name: str,
    queue_map: Dict[str, str],
    new_instance_id: str,
    new_region: str,
    new_account_id: str
) -> Optional[str]:
    """解析新Queue ARN"""
    if display_name and display_name in queue_map:
        arn = queue_map[display_name]
        logger.info(f"  ✓ displayName '{display_name}' 匹配 → {arn}")
        return arn

    if '/queue/' in old_arn:
        queue_id = old_arn.split('/queue/')[-1]
        arn = (
            f"arn:aws:connect:{new_region}:{new_account_id}"
            f":instance/{new_instance_id}/queue/{queue_id}"
        )
        logger.warning(
            f"  ⚠️  displayName '{display_name}' 未匹配到Queue，"
            f"使用重建ARN: {arn}"
        )
        return arn

    logger.error(f"  ❌ 无法解析Queue ARN: {old_arn}")
    return None


def update_flow_ai_agent_info(
    flow_content: Dict,
    created_agent: Dict,
    ai_agent_name: str
) -> Dict:
    """将Flow中所有AI Agent相关字段替换为新AI Agent信息"""
    updated       = copy.deepcopy(flow_content)
    new_agent_arn = created_agent['aiAgentArn']
    assistant_arn = created_agent['assistantArn']

    logger.info(f"新 AI Agent ARN: {new_agent_arn}")
    logger.info(f"Assistant ARN:   {assistant_arn}")

    stats = dict(
        meta_arn=0, meta_name=0, meta_param_arn=0,
        action_wisdom=0, action_lex=0
    )

    for action_id, meta in (
        updated.get('Metadata', {}).get('ActionMetadata', {}).items()
    ):
        if 'aiAgentVersionArn' in meta:
            meta['aiAgentVersionArn'] = new_agent_arn
            stats['meta_arn'] += 1
            logger.info(f"  [Meta][{action_id}] aiAgentVersionArn → 已更新")

        if 'aiAgentName' in meta:
            meta['aiAgentName'] = ai_agent_name
            stats['meta_name'] += 1
            logger.info(f"  [Meta][{action_id}] aiAgentName → {ai_agent_name}")

        wp = meta.get('parameters', {}).get('WisdomAssistantArn', {})
        if wp:
            if 'aiAgentVersionArn' in wp:
                wp['aiAgentVersionArn'] = new_agent_arn
                stats['meta_param_arn'] += 1
                logger.info(
                    f"  [Meta][{action_id}] "
                    f"parameters.WisdomAssistantArn.aiAgentVersionArn → 已更新"
                )
            if 'aiAgentName' in wp:
                wp['aiAgentName'] = ai_agent_name
                logger.info(
                    f"  [Meta][{action_id}] "
                    f"parameters.WisdomAssistantArn.aiAgentName → {ai_agent_name}"
                )

    for action in updated.get('Actions', []):
        atype  = action.get('Type', '')
        aid    = action.get('Identifier', '')
        params = action.get('Parameters', {})

        if atype == 'CreateWisdomSession' and 'WisdomAssistantArn' in params:
            old = params['WisdomAssistantArn']
            params['WisdomAssistantArn'] = assistant_arn
            stats['action_wisdom'] += 1
            logger.info(f"  [Action][{aid}] CreateWisdomSession.WisdomAssistantArn")
            logger.info(f"    旧: {old}")
            logger.info(f"    新: {assistant_arn}")

        if atype == 'ConnectParticipantWithLexBot':
            key       = 'x-amz-lex:q-in-connect:ai-agent-arn'
            lex_attrs = params.get('LexSessionAttributes', {})
            if key in lex_attrs:
                old = lex_attrs[key]
                lex_attrs[key] = new_agent_arn
                stats['action_lex'] += 1
                logger.info(f"  [Action][{aid}] LexSessionAttributes.{key}")
                logger.info(f"    旧: {old}")
                logger.info(f"    新: {new_agent_arn}")

    logger.info(
        f"AI Agent替换统计 → "
        f"Meta ARN:{stats['meta_arn']} Name:{stats['meta_name']} "
        f"ParamARN:{stats['meta_param_arn']} | "
        f"Action Wisdom:{stats['action_wisdom']} Lex:{stats['action_lex']}"
    )
    return updated


def replace_instance_arns_in_flow(
    flow_content: Dict,
    old_instance_id: str,
    new_instance_id: str,
    new_region: str,
    new_account_id: str,
    queue_map: Dict[str, str]
) -> Dict:
    """替换Flow中所有旧Connect实例相关ARN"""
    logger.info("替换Flow中的旧实例ARN...")

    queue_refs = extract_queue_refs(flow_content)

    if not queue_refs:
        logger.info("  未发现 UpdateContactTargetQueue Action，跳过Queue ARN替换")
    else:
        for ref in queue_refs:
            action_id    = ref['action_id']
            old_arn      = ref['queue_arn']
            display_name = ref['display_name']

            new_arn = resolve_queue_arn(
                old_arn=old_arn,
                display_name=display_name,
                queue_map=queue_map,
                new_instance_id=new_instance_id,
                new_region=new_region,
                new_account_id=new_account_id
            )

            if new_arn is None:
                available = ', '.join(queue_map.keys()) or '(无)'
                raise ValueError(
                    f"无法为Queue '{display_name}' 找到新实例对应的ARN。\n"
                    f"  旧ARN: {old_arn}\n"
                    f"  新实例可用Queue: {available}\n"
                    f"  请确认实例 '{new_instance_id}' 中存在名为 '{display_name}' 的Queue。\n"
                    f"  可运行: aws connect list-queues "
                    f"--instance-id {new_instance_id} --region {new_region}"
                )

            for action in flow_content.get('Actions', []):
                if (action.get('Type') == 'UpdateContactTargetQueue'
                        and action.get('Identifier') == action_id):
                    action['Parameters']['QueueId'] = new_arn
                    logger.info(f"  [Action][{action_id}] QueueId 替换完成")
                    logger.info(f"    旧: {old_arn}")
                    logger.info(f"    新: {new_arn}")

    flow_str   = json.dumps(flow_content)
    old_prefix = None

    for ref in queue_refs:
        arn = ref.get('queue_arn', '')
        if arn and 'instance/' in arn:
            parts = arn.split(':')
            if len(parts) >= 6:
                old_prefix = (
                    f"arn:aws:connect:{parts[3]}:{parts[4]}"
                    f":instance/{old_instance_id}"
                )
                break

    if not old_prefix:
        pattern = (
            r'arn:aws:connect:[a-z0-9-]+:\d+:instance/'
            + re.escape(old_instance_id)
        )
        m = re.search(pattern, flow_str)
        if m:
            old_prefix = m.group(0)

    if old_prefix:
        new_prefix = (
            f"arn:aws:connect:{new_region}:{new_account_id}"
            f":instance/{new_instance_id}"
        )
        count = flow_str.count(old_prefix)
        if count > 0:
            flow_str = flow_str.replace(old_prefix, new_prefix)
            logger.info(f"  全文替换旧实例ARN前缀 ({count} 处)")
            logger.info(f"    旧: {old_prefix}")
            logger.info(f"    新: {new_prefix}")
        else:
            logger.info(f"  未发现旧实例ARN前缀，无需全文替换")
    else:
        logger.info("  未能确定旧实例ARN前缀，跳过全文替换")

    logger.info("实例ARN替换完成")
    return json.loads(flow_str)


# ============================================================
# Contact Flow 创建
# ============================================================

FLOW_TYPE_MAP = {
    'contactFlow':     'CONTACT_FLOW',
    'customerQueue':   'CUSTOMER_QUEUE',
    'customerHold':    'CUSTOMER_HOLD',
    'customerWhisper': 'CUSTOMER_WHISPER',
    'agentHold':       'AGENT_HOLD',
    'agentWhisper':    'AGENT_WHISPER',
    'transferToAgent': 'TRANSFER_TO_AGENT',
    'transferToQueue': 'TRANSFER_TO_QUEUE',
    'agentTransfer':   'AGENT_TRANSFER',
    'outboundWhisper': 'OUTBOUND_WHISPER',
}


def create_contact_flow(
    instance_id: str,
    flow_name: str,
    flow_content: Dict,
    region: Optional[str] = None
) -> Dict:
    """调用 connect.create_contact_flow API"""
    client = make_connect_client(region)

    if 'Metadata' in flow_content:
        flow_content['Metadata']['name'] = flow_name

    metadata_type = flow_content.get('Metadata', {}).get('type', 'contactFlow')
    flow_type     = FLOW_TYPE_MAP.get(metadata_type, 'CONTACT_FLOW')
    description   = flow_content.get('Metadata', {}).get('description', '')

    logger.info(f"调用 create_contact_flow:")
    logger.info(f"  名称: {flow_name}")
    logger.info(f"  类型: {flow_type}")
    logger.info(f"  描述: {description or '(无)'}")

    params = {
        'InstanceId': instance_id,
        'Name':       flow_name,
        'Type':       flow_type,
        'Content':    json.dumps(flow_content),
    }
    if description:
        params['Description'] = description

    response = client.create_contact_flow(**params)
    flow_id  = response.get('ContactFlowId')
    flow_arn = response.get('ContactFlowArn')

    logger.info(f"✓ Contact Flow创建成功")
    logger.info(f"  ID:  {flow_id}")
    logger.info(f"  ARN: {flow_arn}")

    return {
        'ContactFlowId':  flow_id,
        'ContactFlowArn': flow_arn,
        'Name':           flow_name,
        'Type':           flow_type,
    }


# ============================================================
# 主部署流程
# ============================================================

def deploy(config: Dict) -> Dict:
    """完整部署流程"""
    assistant_id   = config['assistant_id']
    instance_id    = config['instance_id']
    region         = config['region']
    ai_agent_name  = config['ai_agent_name']
    flow_name      = config['flow_name']
    ai_agent_json  = config['ai_agent_json']
    flow_json      = config['flow_json']
    output_file    = config['output_file']

    account_id = get_account_id(region)

    logger.info("=" * 60)
    logger.info("开始部署 AI Agent + Contact Flow")
    logger.info("=" * 60)
    logger.info(f"  assistant_id:  {assistant_id}")
    logger.info(f"  instance_id:   {instance_id}")
    logger.info(f"  region:        {region}")
    logger.info(f"  account_id:    {account_id}")
    logger.info(f"  ai_agent_name: {ai_agent_name}")
    logger.info(f"  flow_name:     {flow_name}")
    logger.info(f"  ai_agent_json: {ai_agent_json}")
    logger.info(f"  flow_json:     {flow_json}")
    logger.info(f"  boto3版本:     {boto3.__version__}")

    result = {}

    logger.info("\n" + "─" * 40)
    logger.info("Step 1: 创建 AI Agent")
    logger.info("─" * 40)

    agent_resp        = create_ai_agent_from_json(
        json_file=ai_agent_json,
        assistant_id=assistant_id,
        ai_agent_name=ai_agent_name,
        instance_id=instance_id,
        region=region,
        account_id=account_id
    )
    created_agent     = agent_resp.get('aiAgent', {})
    result['aiAgent'] = created_agent

    logger.info("\n" + "─" * 40)
    logger.info("Step 2: 构建新实例 Queue 映射表")
    logger.info("─" * 40)

    queue_map = build_queue_name_to_arn_map(instance_id, region)

    logger.info("\n" + "─" * 40)
    logger.info("Step 3: 更新 Flow 中的 AI Agent 信息")
    logger.info("─" * 40)

    flow_config  = load_json_file(flow_json)
    updated_flow = update_flow_ai_agent_info(
        flow_content=flow_config,
        created_agent=created_agent,
        ai_agent_name=ai_agent_name
    )

    logger.info("\n" + "─" * 40)
    logger.info("Step 4: 替换 Flow 中的旧实例 ARN")
    logger.info("─" * 40)

    old_instance_id = None
    for action in flow_config.get('Actions', []):
        if action.get('Type') == 'UpdateContactTargetQueue':
            arn = action.get('Parameters', {}).get('QueueId', '')
            if arn and 'instance/' in arn:
                old_instance_id = arn.split('instance/')[-1].split('/')[0]
                logger.info(f"从Flow解析旧实例ID: {old_instance_id}")
                break

    if old_instance_id:
        updated_flow = replace_instance_arns_in_flow(
            flow_content=updated_flow,
            old_instance_id=old_instance_id,
            new_instance_id=instance_id,
            new_region=region,
            new_account_id=account_id,
            queue_map=queue_map
        )
    else:
        logger.info("Flow中未发现旧实例ID，跳过实例ARN替换")

    logger.info("\n" + "─" * 40)
    logger.info("Step 5: 创建 Contact Flow")
    logger.info("─" * 40)

    created_flow          = create_contact_flow(
        instance_id=instance_id,
        flow_name=flow_name,
        flow_content=updated_flow,
        region=region
    )
    result['contactFlow'] = created_flow

    logger.info("\n" + "=" * 60)
    logger.info("✅ 部署完成")
    logger.info("=" * 60)
    logger.info("AI Agent:")
    logger.info(f"  名称: {created_agent.get('name')}")
    logger.info(f"  ID:   {created_agent.get('aiAgentId')}")
    logger.info(f"  ARN:  {created_agent.get('aiAgentArn')}")
    logger.info("Contact Flow:")
    logger.info(f"  名称: {created_flow.get('Name')}")
    logger.info(f"  ID:   {created_flow.get('ContactFlowId')}")
    logger.info(f"  ARN:  {created_flow.get('ContactFlowArn')}")

    save_json_file(result, output_file)
    return result


# ============================================================
# 错误提示
# ============================================================

ERROR_HINTS = {
    'UnauthorizedException':       "IAM权限不足，需要: qconnect:CreateAIAgent, connect:CreateContactFlow, connect:ListQueues",
    'AccessDeniedException':       "IAM权限不足，需要: qconnect:CreateAIAgent, connect:CreateContactFlow, connect:ListQueues",
    'ConflictException':           "同名资源已存在，请修改 ai_agent_name 或 flow_name",
    'DuplicateResourceException':  "Flow名称已存在，请修改 flow_name",
    'ResourceNotFoundException':   "资源不存在，请确认 assistant_id / instance_id 及 region 是否正确",
    'InvalidRequestException':     "参数或Flow内容格式不正确，请检查ARN是否全部替换完毕",
    'ValidationException':         "参数格式不正确，请检查配置",
    'InvalidContactFlowException': "Flow JSON格式无效，请检查 flow_json 文件",
}


def print_error_hint(error_msg: str):
    """根据错误信息打印针对性提示"""
    for key, hint in ERROR_HINTS.items():
        if key in error_msg:
            logger.error(f"  提示: {hint}")
            return
    logger.error(f"  请检查日志获取更多信息")


# ============================================================
# 入口
# ============================================================

if __name__ == '__main__':
    config = build_config(sys.argv[1:])

    logger.info("最终运行配置:")
    for k, v in config.items():
        logger.info(f"  {k} = {v}")

    try:
        deploy(config)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"\n❌ 部署失败: {error_msg}")
        print_error_hint(error_msg)
        sys.exit(1)
