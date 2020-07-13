import pdb
from datetime import datetime, timezone
from dateutil.relativedelta import *

COMPASS_APP_NAME_PATTERN = 'MongoDB Compass'
VS_CODE_NAME_PATTERN = 'vscode'

def dates():
    today = datetime.today()
    first_day_of_this_month = \
    datetime(today.year, today.month, 1,tzinfo=timezone.utc)
    first_day_of_last_month = first_day_of_this_month + relativedelta(months=-1)
    end_of_last_month = first_day_of_this_month + relativedelta(days=-1)
    first_day_of_last_month = first_day_of_last_month.strftime('%Y-%m-%d')
    first_day_of_this_month = first_day_of_this_month.strftime('%Y-%m-%d')
    end_of_last_month = end_of_last_month.strftime('%Y-%m-%d')
    return  (first_day_of_last_month,end_of_last_month,first_day_of_this_month)


def query_template_tools(tool):
    start_of_month,end_of_month,start_of_next_month = dates()
    query = """
    WITH dates AS (\
            SELECT\
                DATE(date_column) AS usage_date\
            FROM\
                (\
            VALUES (SEQUENCE(CAST('{1}' AS DATE), CAST('{3}' AS DATE), INTERVAL '1' DAY) ) ) AS t1(date_array)\
            CROSS JOIN UNNEST(date_array) AS t2(date_column) ),\
        daily_usage AS(\
            SELECT\
                usage_date,\
                org_id.oid AS org_id,\
                CASE WHEN sku_name = 'NDS_ENTITLEMENTS' THEN 'support' ELSE 'infrastructure' END AS infra_or_support,\
                SUM(daily_net_revenue) AS daily_net_revenue\
            FROM\
                remodel_cloud.dw__reporting__atlas_billing_sku_daily_paid\
            WHERE\
                usage_date >= DATE('{1}')\
            AND daily_net_revenue > 0\
            GROUP BY 1, 2, 3),\
        date_scaffold AS (\
            SELECT\
                *\
            FROM\
                dates\
            CROSS JOIN (\
                SELECT\
                    org_id,\
                    infra_or_support\
                FROM\
                    daily_usage\
                GROUP BY 1, 2)),\
        trailing_usage AS (\
            SELECT\
                date_scaffold.*,\
                COALESCE(daily_usage.daily_net_revenue, 0) AS daily_net_revenue,\
                sum(COALESCE(daily_usage.daily_net_revenue, 0)) over \
                (PARTITION BY (date_scaffold.org_id, date_scaffold.infra_or_support) \
                ORDER BY date_scaffold.usage_date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS trailing_30_usage\
            FROM\
                daily_usage\
            FULL OUTER JOIN date_scaffold\
            USING (usage_date, org_id, infra_or_support)\
            WHERE date_scaffold.org_id IS NOT NULL\
            AND date_scaffold.infra_or_support IS NOT NULL),\
          revenues as (\
          SELECT sum(trailing_30_usage) as last_30_days, org_id,usage_date FROM trailing_usage where usage_date = date '{2}'\
          group by org_id, usage_date ),\
          vs_usage as (select distinct gid__oid, date_trunc('month',rt) as month from\
           cloud_backend_raw.dw__cloud_backend__rawclientmetadata       where       entries__raw__application__name like '%{0}%' and processed_date >= '{1}' and processed_date <= '{3}'      \
            and rt >= date '{1}' and rt < date '{3}'),\
          all_usage as (\
            select distinct gid__oid as group_id from\
           cloud_backend_raw.dw__cloud_backend__rawclientmetadata  where    processed_date >= '{1}' and processed_date <= '{3}'  and rt >= date '{1}' and rt < date '{3}'\
            ),\
          groups_orgs as (select group_id.oid as group_id, group_org_id.oid as org_id from remodel_cloud.dw__cloud_backend__cloud_groups\
               where group_internal_flag=FALSE),\
          activity_vs_free_paid_join as (\
             select groups_orgs.org_id as total_org_id, vs_usage.month as month, coalesce(last_30_days,0) as last_30_days, vs_usage.gid__oid as usage_group_id, revenues.org_id as revenues_org_id from\
          (\
          all_usage\
            join groups_orgs on all_usage.group_id = groups_orgs.group_id\
          left join vs_usage on groups_orgs.group_id = vs_usage.gid__oid\
           left join revenues on groups_orgs.org_id = revenues.org_id)\
          )\
          select\
          max(month) as month,\
           count(distinct (case when last_30_days > 0 and usage_group_id is not NULL then total_org_id end)) as num_paid_orgs_tool_usage,\
            count(distinct (case when (last_30_days = 0) and usage_group_id is not NULL  then total_org_id end)) as num_free_orgs_tool_usage,\
             count(distinct (case when last_30_days > 0  then total_org_id end)) as num_paid_orgs_total,\
            count(distinct (case when (last_30_days = 0) then total_org_id end)) as num_free_orgs_total\
      from activity_vs_free_paid_join""".format(tool,start_of_month,end_of_month,start_of_next_month)
    print(query)
    return query

def query_compass():
    return query_template_tools(COMPASS_APP_NAME_PATTERN)

def query_vscode():
    return query_template_tools(VS_CODE_NAME_PATTERN)

def collections_queries():
    return [
        {'compass_monthly_free_paid': query_compass()}#,
    #    {'vscode_monthly_free_paid': query_vscode()}
    ]
