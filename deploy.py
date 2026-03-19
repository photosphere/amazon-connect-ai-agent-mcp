import boto3
import json
import copy
import uuid
import logging
import sys
from datetime import datetime
from typing import Dict, Optional, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        raise ValueError(f"JSON文件格式错误: {file_path}, 错误: {e}")


def save_json_file(data: Dict, filename: str = None) -> str:
    """保存结果到JSON文件"""
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'deploy_result_{timestamp}.json'

    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    output_data = {
        'timestamp': datetime.now().isoformat(),
        'data': data
    }
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2,
                  default=json_serializer)

    logger.info(f"结果已保存到: {filename}")
    return filename


def get_current_region(region_name: Optional[str] = None) -> str:
    """获取当前AWS区域"""
    if region_name:
        return region_name
    session = boto3.Session()
    return session.region_name or 'us-east-1'


def get_account_id(region_name: Optional[str] = None) -> str:
    """获取当前AWS账号ID"""
    client = boto3.client('sts', region_name=region_name) if region_name \
        else boto3.client('sts')
    return client.get_caller_identity()['Account']


# ============================================================
# Connect Queue 查询函数
# ============================================================

def list_all_queues(
    instance_id: str,
    region_name: Optional[str] = None
) -> List[Dict]:
    """
    列出Connect实例下所有Queue（自动翻页）

    QueueSummary 字段结构参考:
      https://docs.aws.amazon.com/connect/latest/APIReference/API_QueueSummary.html
      - Id       : Queue ID
      - Arn      : Queue ARN
      - Name     : Queue名称（可能不存在于某些AGENT类型）
      - QueueType: STANDARD | AGENT

    Args:
        instance_id: Connect实例ID
        region_name: AWS区域

    Returns:
        原始Queue列表
    """
    client = boto3.client('connect', region_name=region_name) if region_name \
        else boto3.client('connect')

    all_queues = []
    paginator  = client.get_paginator('list_queues')

    for page in paginator.paginate(
        InstanceId=instance_id,
        QueueTypes=['STANDARD', 'AGENT']
    ):
        page_queues = page.get('QueueSummaryList', [])
        all_queues.extend(page_queues)

    logger.info(f"实例 {instance_id} 共找到 {len(all_queues)} 个Queue")

    # 打印字段结构（取第一条用于调试）
    if all_queues:
        sample = all_queues[0]
        logger.info(f"Queue字段示例: {json.dumps(sample, default=str)}")

    return all_queues


def build_queue_name_to_arn_map(
    instance_id: str,
    region_name: Optional[str] = None
) -> Dict[str, str]:
    """
    构建 Queue名称 → Queue ARN 映射表

    防御性处理：
    - 兼容字段名大小写差异（Name/name, Arn/arn）
    - 跳过无Name或无ARN的条目并记录警告
    - AGENT类型Queue通常无Name，正常跳过

    Args:
        instance_id: Connect实例ID
        region_name: AWS区域

    Returns:
        { queue_name: queue_arn }
    """
    queues  = list_all_queues(instance_id, region_name)
    mapping = {}
    skipped = []

    for q in queues:
        # 兼容大小写
        name       = q.get('Name') or q.get('name') or ''
        arn        = q.get('Arn')  or q.get('arn')  or ''
        queue_type = q.get('QueueType') or q.get('queueType') or ''

        if not name:
            # AGENT类型Queue无Name属于正常情况
            if queue_type != 'AGENT':
                logger.warning(
                    f"  跳过无Name的Queue "
                    f"[type={queue_type}]: {json.dumps(q, default=str)}"
                )
            skipped.append(q)
            continue

        if not arn:
            logger.warning(f"  跳过无ARN的Queue: name={name}")
            skipped.append(q)
            continue

        mapping[name] = arn

    logger.info(f"Queue映射表构建完成: {len(mapping)} 条有效, {len(skipped)} 条跳过")
    logger.info("Queue名称→ARN映射:")
    for name, arn in mapping.items():
        logger.info(f"  [{name}] → {arn}")

    return mapping


# ============================================================
# AI Agent 相关函数
# ============================================================

def load_ai_agent_config(json_file: str) -> Dict:
    """从JSON文件加载AI Agent配置"""
    data     = load_json_file(json_file)
    ai_agent = data.get('data', {}).get('aiAgent', {})

    if not ai_agent:
        raise ValueError(
            f"JSON文件 '{json_file}' 中未找到 data.aiAgent 配置"
        )

    logger.info(f"成功加载AI Agent配置: {json_file}")
    logger.info(f"  原始名称: {ai_agent.get('name')}")
    logger.info(f"  类型:     {ai_agent.get('type')}")
    return ai_agent


def verify_assistant_exists(
    assistant_id: str,
    region_name: Optional[str] = None
) -> bool:
    """验证Assistant是否存在"""
    client = boto3.client('qconnect', region_name=region_name) if region_name \
        else boto3.client('qconnect')
    try:
        client.get_assistant(assistantId=assistant_id)
        return True
    except client.exceptions.ResourceNotFoundException:
        return False
    except Exception as e:
        logger.warning(f"验证Assistant时出错: {e}")
        return False


def update_connect_instance_arn(
    configuration: Dict,
    connect_instance_id: str,
    region_name: Optional[str] = None
) -> Dict:
    """更新配置中的Connect实例ARN"""
    current_region = get_current_region(region_name)
    account_id     = get_account_id(region_name)
    new_arn        = (
        f"arn:aws:connect:{current_region}:{account_id}"
        f":instance/{connect_instance_id}"
    )

    if 'orchestrationAIAgentConfiguration' in configuration:
        old_arn = configuration['orchestrationAIAgentConfiguration'].get(
            'connectInstanceArn', 'N/A'
        )
        configuration['orchestrationAIAgentConfiguration']['connectInstanceArn'] = new_arn
        logger.info(f"更新Connect实例ARN:")
        logger.info(f"  旧ARN: {old_arn}")
        logger.info(f"  新ARN: {new_arn}")

    return configuration


def validate_configuration(
    configuration: Dict,
    region_name: Optional[str] = None
) -> Tuple[bool, str]:
    """验证AI Agent配置是否有效"""
    if 'orchestrationAIAgentConfiguration' not in configuration:
        return True, ""

    connect_arn = configuration['orchestrationAIAgentConfiguration'].get(
        'connectInstanceArn'
    )
    if not connect_arn:
        return False, "配置中缺少 connectInstanceArn"

    try:
        arn_parts      = connect_arn.split(':')
        arn_region     = arn_parts[3]
        instance_id    = arn_parts[5].split('/')[-1]
        current_region = get_current_region(region_name)

        if arn_region != current_region:
            return False, (
                f"Connect实例ARN区域({arn_region})"
                f"与当前区域({current_region})不匹配"
            )

        connect_client = boto3.client('connect', region_name=region_name) \
            if region_name else boto3.client('connect')
        connect_client.describe_instance(InstanceId=instance_id)
        return True, ""

    except Exception as e:
        err = str(e)
        if 'ResourceNotFoundException' in err:
            return False, f"Connect实例 '{instance_id}' 不存在"
        return False, f"验证Connect实例时出错: {err}"


def create_ai_agent(
    assistant_id: str,
    name: str,
    agent_type: str,
    configuration: Dict,
    visibility_status: str = 'PUBLISHED',
    description: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    region_name: Optional[str] = None
) -> Dict:
    """调用 qconnect API 创建 AI Agent"""
    client = boto3.client('qconnect', region_name=region_name) if region_name \
        else boto3.client('qconnect')

    params = {
        'assistantId':      assistant_id,
        'name':             name,
        'type':             agent_type,
        'configuration':    configuration,
        'visibilityStatus': visibility_status,
        'clientToken':      str(uuid.uuid4())
    }
    if description:
        params['description'] = description
    if tags:
        params['tags'] = tags

    logger.info(f"正在创建AI Agent...")
    logger.info(f"  名称:   {name}")
    logger.info(f"  类型:   {agent_type}")
    logger.info(f"  可见性: {visibility_status}")

    response = client.create_ai_agent(**params)
    ai_agent = response.get('aiAgent', {})

    logger.info(f"✓ AI Agent创建成功!")
    logger.info(f"  ID:  {ai_agent.get('aiAgentId')}")
    logger.info(f"  ARN: {ai_agent.get('aiAgentArn')}")
    return response


def create_ai_agent_from_json(
    json_file: str,
    target_assistant_id: str,
    new_name: Optional[str] = None,
    region_name: Optional[str] = None,
    connect_instance_id: Optional[str] = None
) -> Dict:
    """从JSON文件创建AI Agent（完整流程）"""
    current_region = get_current_region(region_name)

    logger.info(f"正在加载配置文件: {json_file}")
    ai_agent_config = load_ai_agent_config(json_file)

    name              = new_name or ai_agent_config.get('name')
    agent_type        = ai_agent_config.get('type')
    configuration     = copy.deepcopy(ai_agent_config.get('configuration'))
    visibility_status = ai_agent_config.get('visibilityStatus', 'PUBLISHED')
    description       = ai_agent_config.get('description')
    tags              = ai_agent_config.get('tags', {})

    logger.info(f"配置信息:")
    logger.info(f"  原始名称:        {ai_agent_config.get('name')}")
    logger.info(f"  新名称:          {name}")
    logger.info(f"  类型:            {agent_type}")
    logger.info(f"  目标AssistantID: {target_assistant_id}")

    # 验证Assistant
    logger.info(f"正在验证Assistant是否存在...")
    if verify_assistant_exists(target_assistant_id, region_name):
        logger.info(f"✓ Assistant验证成功: {target_assistant_id}")
    else:
        raise ValueError(
            f"Assistant '{target_assistant_id}' 不存在于区域 '{current_region}'"
        )

    # 更新Connect实例ARN
    if connect_instance_id:
        logger.info(f"使用指定的Connect实例ID更新ARN: {connect_instance_id}")
        configuration = update_connect_instance_arn(
            configuration, connect_instance_id, region_name
        )
    else:
        logger.info("使用配置文件中的Connect实例ARN（不替换）")

    # 验证配置
    logger.info(f"正在验证配置...")
    is_valid, error_msg = validate_configuration(configuration, region_name)
    if is_valid:
        logger.info(f"✓ 配置验证成功")
    else:
        raise ValueError(f"配置验证失败: {error_msg}")

    return create_ai_agent(
        assistant_id=target_assistant_id,
        name=name,
        agent_type=agent_type,
        configuration=configuration,
        visibility_status=visibility_status,
        description=description,
        tags=tags,
        region_name=region_name
    )


# ============================================================
# Flow 更新函数
# ============================================================

def extract_queue_refs_from_flow(flow_content: Dict) -> List[Dict]:
    """
    从Flow中提取所有 UpdateContactTargetQueue Action 的 QueueId 信息

    Returns:
        [{ action_id, queue_arn, display_name }]
    """
    found = []
    for action in flow_content.get('Actions', []):
        if action.get('Type') != 'UpdateContactTargetQueue':
            continue

        action_id = action.get('Identifier', '')
        queue_arn = action.get('Parameters', {}).get('QueueId', '')

        if not queue_arn:
            continue

        # 从 Metadata 获取 displayName
        display_name = (
            flow_content
            .get('Metadata', {})
            .get('ActionMetadata', {})
            .get(action_id, {})
            .get('parameters', {})
            .get('QueueId', {})
            .get('displayName', '')
        )

        found.append({
            'action_id':    action_id,
            'queue_arn':    queue_arn,
            'display_name': display_name
        })
        logger.info(
            f"  发现Queue引用: "
            f"action={action_id}, displayName='{display_name}', arn={queue_arn}"
        )

    return found


def resolve_new_queue_arn(
    old_queue_arn: str,
    display_name: str,
    queue_name_to_arn: Dict[str, str],
    new_instance_id: str,
    new_region: str,
    new_account_id: str
) -> Optional[str]:
    """
    解析新Queue ARN，优先级：
      1. 通过 displayName 匹配新实例Queue
      2. 重建ARN（保留Queue ID，替换实例ID和区域）
    """
    # 策略1: displayName 精确匹配
    if display_name and display_name in queue_name_to_arn:
        new_arn = queue_name_to_arn[display_name]
        logger.info(
            f"  ✓ 通过displayName '{display_name}' 匹配到新Queue: {new_arn}"
        )
        return new_arn

    # 策略2: 从旧ARN提取Queue ID，重建新ARN
    # 旧ARN格式: arn:aws:connect:region:account:instance/INST_ID/queue/QUEUE_ID
    try:
        if '/queue/' in old_queue_arn:
            old_queue_id = old_queue_arn.split('/queue/')[-1]
            new_arn = (
                f"arn:aws:connect:{new_region}:{new_account_id}"
                f":instance/{new_instance_id}/queue/{old_queue_id}"
            )
            logger.warning(
                f"  ⚠️  displayName '{display_name}' 未匹配到Queue，"
                f"使用重建ARN（保留Queue ID）: {new_arn}"
            )
            return new_arn
    except Exception as e:
        logger.error(f"  重建Queue ARN失败: {e}")

    return None


def update_flow_ai_agent_info(
    flow_content: Dict,
    created_agent: Dict,
    ai_agent_name: str
) -> Dict:
    """将Flow中AI Agent相关字段替换为新创建的AI Agent信息"""
    logger.info("开始更新Flow中的AI Agent配置...")

    updated_flow  = copy.deepcopy(flow_content)
    new_agent_arn = created_agent['aiAgentArn']
    assistant_arn = created_agent['assistantArn']

    logger.info(f"  新AI Agent ARN: {new_agent_arn}")
    logger.info(f"  Assistant ARN:  {assistant_arn}")

    stats = {
        'metadata_top_arn':   0,
        'metadata_top_name':  0,
        'metadata_param_arn': 0,
        'action_wisdom':      0,
        'action_lex':         0,
    }

    # 更新 Metadata.ActionMetadata
    action_metadata = updated_flow.get('Metadata', {}).get('ActionMetadata', {})
    for action_id, meta in action_metadata.items():
        if 'aiAgentVersionArn' in meta:
            meta['aiAgentVersionArn'] = new_agent_arn
            stats['metadata_top_arn'] += 1
            logger.info(
                f"  [Metadata][{action_id}] aiAgentVersionArn → 已更新"
            )
        if 'aiAgentName' in meta:
            meta['aiAgentName'] = ai_agent_name
            stats['metadata_top_name'] += 1
            logger.info(
                f"  [Metadata][{action_id}] aiAgentName → {ai_agent_name}"
            )
        wisdom_param = meta.get('parameters', {}).get('WisdomAssistantArn', {})
        if wisdom_param:
            if 'aiAgentVersionArn' in wisdom_param:
                wisdom_param['aiAgentVersionArn'] = new_agent_arn
                stats['metadata_param_arn'] += 1
                logger.info(
                    f"  [Metadata][{action_id}] "
                    f"parameters.WisdomAssistantArn.aiAgentVersionArn → 已更新"
                )
            if 'aiAgentName' in wisdom_param:
                wisdom_param['aiAgentName'] = ai_agent_name
                logger.info(
                    f"  [Metadata][{action_id}] "
                    f"parameters.WisdomAssistantArn.aiAgentName → {ai_agent_name}"
                )

    # 更新 Actions
    for action in updated_flow.get('Actions', []):
        action_type = action.get('Type', '')
        action_id   = action.get('Identifier', '')
        parameters  = action.get('Parameters', {})

        if action_type == 'CreateWisdomSession' and 'WisdomAssistantArn' in parameters:
            old = parameters['WisdomAssistantArn']
            parameters['WisdomAssistantArn'] = assistant_arn
            stats['action_wisdom'] += 1
            logger.info(
                f"  [Action][{action_id}] CreateWisdomSession.WisdomAssistantArn"
            )
            logger.info(f"    旧: {old}")
            logger.info(f"    新: {assistant_arn}")

        if action_type == 'ConnectParticipantWithLexBot':
            lex_key   = 'x-amz-lex:q-in-connect:ai-agent-arn'
            lex_attrs = parameters.get('LexSessionAttributes', {})
            if lex_key in lex_attrs:
                old = lex_attrs[lex_key]
                lex_attrs[lex_key] = new_agent_arn
                stats['action_lex'] += 1
                logger.info(
                    f"  [Action][{action_id}] LexSessionAttributes.{lex_key}"
                )
                logger.info(f"    旧: {old}")
                logger.info(f"    新: {new_agent_arn}")

    logger.info(f"Flow AI Agent更新统计:")
    logger.info(f"  Metadata aiAgentVersionArn (顶层):  {stats['metadata_top_arn']} 处")
    logger.info(f"  Metadata aiAgentName (顶层):        {stats['metadata_top_name']} 处")
    logger.info(f"  Metadata params aiAgentVersionArn:  {stats['metadata_param_arn']} 处")
    logger.info(f"  Action CreateWisdomSession:         {stats['action_wisdom']} 处")
    logger.info(f"  Action LexSessionAttributes:        {stats['action_lex']} 处")

    return updated_flow


def replace_instance_arns_in_flow(
    flow_content: Dict,
    old_instance_id: str,
    new_instance_id: str,
    new_region: str,
    new_account_id: str,
    queue_name_to_arn: Dict[str, str]
) -> Dict:
    """
    替换Flow中所有旧Connect实例相关ARN

    策略:
      1. 精确替换 UpdateContactTargetQueue.QueueId（通过displayName匹配）
      2. 全文字符串替换旧实例ARN前缀（兜底）
    """
    logger.info("开始替换Flow中的旧实例ARN...")

    # --------------------------------------------------------
    # Step A: 精确替换 UpdateContactTargetQueue QueueId
    # --------------------------------------------------------
    queue_refs = extract_queue_refs_from_flow(flow_content)

    if not queue_refs:
        logger.info("  Flow中未发现 UpdateContactTargetQueue Action，跳过Queue ARN替换")
    else:
        for ref in queue_refs:
            action_id    = ref['action_id']
            old_arn      = ref['queue_arn']
            display_name = ref['display_name']

            new_arn = resolve_new_queue_arn(
                old_queue_arn=old_arn,
                display_name=display_name,
                queue_name_to_arn=queue_name_to_arn,
                new_instance_id=new_instance_id,
                new_region=new_region,
                new_account_id=new_account_id
            )

            if new_arn is None:
                available = ', '.join(queue_name_to_arn.keys()) or '(无)'
                raise ValueError(
                    f"无法为Queue '{display_name}' 找到新实例对应的ARN。\n"
                    f"旧ARN: {old_arn}\n"
                    f"新实例可用Queue: {available}\n"
                    f"请确认新实例 '{new_instance_id}' 中存在名为 "
                    f"'{display_name}' 的Queue。"
                )

            # 更新 Actions 中的 QueueId
            for action in flow_content.get('Actions', []):
                if (action.get('Type') == 'UpdateContactTargetQueue'
                        and action.get('Identifier') == action_id):
                    action['Parameters']['QueueId'] = new_arn
                    logger.info(
                        f"  [Action][{action_id}] "
                        f"UpdateContactTargetQueue.QueueId"
                    )
                    logger.info(f"    旧: {old_arn}")
                    logger.info(f"    新: {new_arn}")

    # --------------------------------------------------------
    # Step B: 全文替换旧实例ARN前缀（兜底）
    # --------------------------------------------------------
    flow_str = json.dumps(flow_content)

    # 尝试从Queue ARN中提取旧实例信息
    old_prefix = None
    for ref in queue_refs:
        arn = ref.get('queue_arn', '')
        if arn and 'instance/' in arn:
            # arn:aws:connect:REGION:ACCOUNT:instance/INST_ID/queue/...
            parts = arn.split(':')
            if len(parts) >= 6:
                old_region  = parts[3]
                old_account = parts[4]
                old_prefix  = (
                    f"arn:aws:connect:{old_region}:{old_account}"
                    f":instance/{old_instance_id}"
                )
                break

    # 若无Queue引用，用旧instance_id拼接尝试匹配
    if not old_prefix and old_instance_id:
        # 扫描全文找含旧instance_id的ARN前缀
        import re
        pattern = (
            r'arn:aws:connect:[a-z0-9-]+:\d+:instance/'
            + re.escape(old_instance_id)
        )
        match = re.search(pattern, flow_str)
        if match:
            old_prefix = match.group(0)

    if old_prefix:
        new_prefix = (
            f"arn:aws:connect:{new_region}:{new_account_id}"
            f":instance/{new_instance_id}"
        )
        count = flow_str.count(old_prefix)
        if count > 0:
            flow_str = flow_str.replace(old_prefix, new_prefix)
            logger.info(f"  全文替换旧实例ARN前缀 ({count} 处):")
            logger.info(f"    旧: {old_prefix}")
            logger.info(f"    新: {new_prefix}")
        else:
            logger.info(f"  未发现旧实例ARN前缀: {old_prefix}")
    else:
        logger.info("  未能确定旧实例ARN前缀，跳过全文替换")

    logger.info("实例ARN替换完成")
    return json.loads(flow_str)


def create_contact_flow(
    instance_id: str,
    flow_name: str,
    flow_content: Dict,
    region_name: Optional[str] = None
) -> Dict:
    """创建Contact Flow"""
    client = boto3.client('connect', region_name=region_name) if region_name \
        else boto3.client('connect')

    if 'Metadata' in flow_content:
        flow_content['Metadata']['name'] = flow_name

    flow_type_map = {
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

    metadata_type = flow_content.get('Metadata', {}).get('type', 'contactFlow')
    flow_type     = flow_type_map.get(metadata_type, 'CONTACT_FLOW')
    description   = flow_content.get('Metadata', {}).get('description', '')

    logger.info(f"正在创建Contact Flow...")
    logger.info(f"  名称:   {flow_name}")
    logger.info(f"  类型:   {flow_type}")
    logger.info(f"  描述:   {description or '(无)'}")

    create_params = {
        'InstanceId': instance_id,
        'Name':       flow_name,
        'Type':       flow_type,
        'Content':    json.dumps(flow_content),
    }
    if description:
        create_params['Description'] = description

    response = client.create_contact_flow(**create_params)
    flow_id  = response.get('ContactFlowId')
    flow_arn = response.get('ContactFlowArn')

    logger.info(f"✓ Contact Flow创建成功!")
    logger.info(f"  ID:  {flow_id}")
    logger.info(f"  ARN: {flow_arn}")

    return {
        'ContactFlowId':  flow_id,
        'ContactFlowArn': flow_arn,
        'Name':           flow_name,
        'Type':           flow_type,
    }


# ============================================================
# 主流程
# ============================================================

def deploy(
    assistant_id: str,
    instance_id: str,
    ai_agent_name: str,
    flow_name: str,
    ai_agent_json_path: str = 'ai_agent.json',
    flow_json_path: str = 'connect_flow_mcp.json',
    region: Optional[str] = None,
    output_file: Optional[str] = None
) -> Dict:
    """
    完整部署流程:
      Step 1 - 创建 AI Agent
      Step 2 - 构建新实例 Queue 映射表
      Step 3 - 更新 Flow 中的 AI Agent 信息
      Step 4 - 替换 Flow 中的旧实例 ARN
      Step 5 - 创建 Contact Flow
    """
    current_region = get_current_region(region)
    account_id     = get_account_id(region)

    logger.info("=" * 60)
    logger.info("开始部署 AI Agent + Contact Flow")
    logger.info("=" * 60)
    logger.info(f"  Assistant ID:  {assistant_id}")
    logger.info(f"  Instance ID:   {instance_id}")
    logger.info(f"  AI Agent名称:  {ai_agent_name}")
    logger.info(f"  Flow名称:      {flow_name}")
    logger.info(f"  AWS区域:       {current_region}")
    logger.info(f"  AWS账号:       {account_id}")
    logger.info(f"  boto3版本:     {boto3.__version__}")

    result = {}

    # Step 1: 创建 AI Agent
    logger.info("\n" + "=" * 40)
    logger.info("Step 1: 创建 AI Agent")
    logger.info("=" * 40)
    agent_response     = create_ai_agent_from_json(
        json_file=ai_agent_json_path,
        target_assistant_id=assistant_id,
        new_name=ai_agent_name,
        region_name=region,
        connect_instance_id=instance_id
    )
    created_agent      = agent_response.get('aiAgent', {})
    result['aiAgent']  = created_agent

    # Step 2: 构建 Queue 映射表
    logger.info("\n" + "=" * 40)
    logger.info("Step 2: 构建新实例 Queue 映射表")
    logger.info("=" * 40)
    queue_name_to_arn = build_queue_name_to_arn_map(instance_id, region)

    # Step 3: 加载Flow并更新AI Agent信息
    logger.info("\n" + "=" * 40)
    logger.info("Step 3: 更新 Flow 中的 AI Agent 信息")
    logger.info("=" * 40)
    flow_config  = load_json_file(flow_json_path)
    updated_flow = update_flow_ai_agent_info(
        flow_content=flow_config,
        created_agent=created_agent,
        ai_agent_name=ai_agent_name
    )

    # Step 4: 替换旧实例ARN
    logger.info("\n" + "=" * 40)
    logger.info("Step 4: 替换 Flow 中的旧实例 ARN")
    logger.info("=" * 40)

    # 从原始Flow中提取旧实例ID
    old_instance_id = None
    for action in flow_config.get('Actions', []):
        if action.get('Type') == 'UpdateContactTargetQueue':
            queue_arn = action.get('Parameters', {}).get('QueueId', '')
            if queue_arn and 'instance/' in queue_arn:
                old_instance_id = queue_arn.split('instance/')[-1].split('/')[0]
                logger.info(f"从Flow解析到旧实例ID: {old_instance_id}")
                break

    if old_instance_id:
        updated_flow = replace_instance_arns_in_flow(
            flow_content=updated_flow,
            old_instance_id=old_instance_id,
            new_instance_id=instance_id,
            new_region=current_region,
            new_account_id=account_id,
            queue_name_to_arn=queue_name_to_arn
        )
    else:
        logger.info("Flow中未发现旧实例ID，跳过实例ARN替换")

    # Step 5: 创建 Contact Flow
    logger.info("\n" + "=" * 40)
    logger.info("Step 5: 创建 Contact Flow")
    logger.info("=" * 40)
    created_flow          = create_contact_flow(
        instance_id=instance_id,
        flow_name=flow_name,
        flow_content=updated_flow,
        region_name=region
    )
    result['contactFlow'] = created_flow

    # 结果摘要
    logger.info("\n" + "=" * 60)
    logger.info("✅ 部署完成! 结果摘要:")
    logger.info("=" * 60)
    logger.info(f"AI Agent:")
    logger.info(f"  名称: {created_agent.get('name')}")
    logger.info(f"  ID:   {created_agent.get('aiAgentId')}")
    logger.info(f"  ARN:  {created_agent.get('aiAgentArn')}")
    logger.info(f"Contact Flow:")
    logger.info(f"  名称: {created_flow.get('Name')}")
    logger.info(f"  ID:   {created_flow.get('ContactFlowId')}")
    logger.info(f"  ARN:  {created_flow.get('ContactFlowArn')}")

    save_json_file(result, output_file)
    return result


# ============================================================
# 入口
# ============================================================

if __name__ == '__main__':

    CONFIG = {
        'assistant_id':       'f8d5155e-7514-42f9-aca8-5aa224722e8a',
        'instance_id':        'b7e4b4ed-1bdf-4b14-b624-d9328f08725a',
        'ai_agent_name':      'SelfServiceOrchestrator_Query',
        'flow_name':          'MCP Inbound Flow',
        'ai_agent_json_path': 'ai_agent.json',
        'flow_json_path':     'connect_flow_mcp.json',
        'region':             'us-east-1',
        'output_file':        None,
    }

    try:
        deploy(**CONFIG)

    except Exception as e:
        error_msg = str(e)
        logger.error(f"\n❌ 部署失败: {error_msg}")

        if 'UnauthorizedException' in error_msg or 'AccessDenied' in error_msg:
            logger.error("  → 需要权限: qconnect:CreateAIAgent, connect:CreateContactFlow, connect:ListQueues")
        elif 'ConflictException' in error_msg:
            logger.error("  → 同名资源已存在，请修改 ai_agent_name 或 flow_name")
        elif 'ResourceNotFoundException' in error_msg:
            logger.error("  → 资源不存在，请确认 assistant_id / instance_id 及区域")
        elif 'InvalidRequestException' in error_msg or 'ValidationException' in error_msg:
            logger.error("  → 参数或Flow内容格式不正确，请检查ARN是否全部替换完毕")
        elif 'DuplicateResourceException' in error_msg:
            logger.error("  → Flow名称已存在，请修改 flow_name")
        elif 'InvalidContactFlowException' in error_msg:
            logger.error("  → Flow JSON格式无效，请检查 connect_flow_mcp.json")
