import logging

from dtable_events.app.actions import AutomationRule

logger = logging.getLogger(__name__)


def scan_triggered_automation_rules(event_data, db_session):
    # if event_data.get('op_user') == 'Automation Rule':
    #     # For preventing loop do automation actions, foribidden triggering actions!!!
    #     return
    dtable_uuid = event_data.get('dtable_uuid')
    automation_rule_id = event_data.get('automation_rule_id')
    sql = """
        SELECT `id`, `run_condition`, `trigger`, `actions`, `last_trigger_time`, `dtable_uuid` FROM `dtable_automation_rules`
        WHERE dtable_uuid=:dtable_uuid AND run_condition='per_update' AND is_valid=1 AND id=:rule_id
    """
    try:
        rules = db_session.execute(sql, {'dtable_uuid': dtable_uuid, 'rule_id': automation_rule_id}).fetchall()
    except Exception as e:
        logger.error('checkout auto rules error: %s', e)
        return

    for rule_id, run_condition, trigger, actions, last_trigger_time, dtable_uuid in rules:
        try:
            auto_rule = AutomationRule(rule_id, run_condition, dtable_uuid, trigger, actions, last_trigger_time, event_data, db_session)
            auto_rule.do_actions()
        except Exception as e:
            logger.error('auto rule: %s do actions error: %s', rule_id, e)


def run_regular_execution_rule(rule, db_session):
    rule_id = rule[0]
    run_condition = rule[1]
    raw_trigger = rule[2]
    raw_actions = rule[3]
    last_trigger_time = rule[4]
    dtable_uuid = rule[5]
    try:
        auto_rule = AutomationRule(rule_id, run_condition, dtable_uuid, raw_trigger, raw_actions, last_trigger_time, None, db_session)
        auto_rule.do_actions()
    except Exception as e:
        logger.error('auto rule: %s do actions error: %s', rule_id, e)
