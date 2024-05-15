with source_data as (
    select
        order_id,
        status,
        is_cancelled,
        customer_id,
        manufacturer_id,
        customer_price,
        manufacturer_price,
        shipping_price,
        markup,
        account_manager_country,
        created_at,
        in_production_at
    from analytics_engineer_test_task.orders 
),

parts_count as (
    select 
        order_id,
        COUNT(DISTINCT order_part_id) as parts_count
    from analytics_engineer_test_task.parts
    group by order_id

),

cnc_count as ( -- assuming again different parts not total quantity, otherwise SUM(QUANTITY)
    select 
        order_id,
        COUNT(DISTINCT order_part_id) as parts_count
    from analytics_engineer_test_task.parts
    where selected_process_type = 'cnc_machining'
    group by order_id
),

laser_count as ( -- assuming again different parts not total quantity, otherwise SUM(QUANTITY)
    select 
        order_id,
        COUNT(DISTINCT order_part_id) as parts_count
    from analytics_engineer_test_task.parts
    where selected_process_type in ('laser_cutting','laser_tube_cutting')
    group by order_id
),

bending_parts as ( -- assuming again different parts not total quantity, otherwise SUM(QUANTITY)
    select 
        order_id,
        COUNT(DISTINCT order_part_id) as parts_count,
        SUM(bends_count) as bends_count --assuming again we dont take quantity into consideration, otherwise SUM(bends_count * quantity)
    from analytics_engineer_test_task.parts
    where has_bending = 1
    group by order_id
),

surface_coating_count as ( -- assuming again different parts not total quantity, otherwise SUM(QUANTITY)
    select 
        order_id,
        COUNT(DISTINCT order_part_id) as parts_count
    from analytics_engineer_test_task.parts
    where has_surface_coating = true
    group by order_id
),

insert_operations_count as ( -- assuming again different parts not total quantity, otherwise SUM(QUANTITY)
    select 
        order_id,
        COUNT(DISTINCT order_part_id) as parts_count
    from analytics_engineer_test_task.parts
    where has_insert_operations = true
    group by order_id
),
/*
unable to select from {{ ref('marek_parts_ext') }} so i am adding the same cte here.
*/
ral_code_list as (
    select 
        sub.order_id,
        STRING_AGG(sub.ral_code) as ral_code_list
    from (
        select distinct
            parts.order_id,
            JSON_EXTRACT_SCALAR(process_config, '$.ralCode') as ral_code
        from analytics_engineer_test_task.parts_surface_finish_config as ral
        inner join analytics_engineer_test_task.parts as parts on 
            parts.order_part_id = ral.order_part_id
        where process_name = 'SECONDARY_SURFACE_FINISH_RAL'
    ) sub
    group by sub.order_id
),

ral_finish_list as (
    select 
        sub.order_id,
        STRING_AGG(sub.ral_finish) as ral_finish_list
    from (
        select distinct
            parts.order_id,
            JSON_EXTRACT_SCALAR(process_config, '$.ralFinish') as ral_finish
        from analytics_engineer_test_task.parts_surface_finish_config as ral
        inner join analytics_engineer_test_task.parts as parts on 
            parts.order_part_id = ral.order_part_id
        where process_name = 'SECONDARY_SURFACE_FINISH_RAL'
    ) sub 
    group by sub.order_id
),

surface_finish_list as (
    select 
        sub.order_id,
        STRING_AGG(sub.surface_finish) as surface_finish_list
    from (
        select distinct
            parts.order_id,
            JSON_EXTRACT_SCALAR(process_config, '$.value') as surface_finish
        from analytics_engineer_test_task.parts_surface_finish_config as ral
        inner join analytics_engineer_test_task.parts as parts on 
            parts.order_part_id = ral.order_part_id
        where process_name = 'SURFACE_FINISH'
    ) sub 
    group by sub.order_id
),

secondary_surface_finish_list as (
    select 
        sub.order_id,
        STRING_AGG(sub.surface_finish) as secondary_surface_finish_list
    from (
        select distinct
            parts.order_id,
            JSON_EXTRACT_SCALAR(process_config, '$.value') as surface_finish
        from analytics_engineer_test_task.parts_surface_finish_config as ral
        inner join analytics_engineer_test_task.parts as parts on 
            parts.order_part_id = ral.order_part_id
        where process_name = 'SECONDARY_SURFACE_FINISH'
    ) sub 
    group by sub.order_id
),

final as (
    select
        source_data.order_id,
        source_data.status,
        source_data.is_cancelled,
        source_data.customer_id,
        source_data.manufacturer_id,
        source_data.customer_price,
        source_data.manufacturer_price,
        source_data.shipping_price,
        source_data.markup,
        source_data.account_manager_country,
        source_data.created_at,
        source_data.in_production_at,
        parts_count.parts_count,
        cnc_count.parts_count as cnc_parts_count,
        laser_count.parts_count as laser_parts_count,
        bending_parts.parts_count as bending_parts_count,
        bending_parts.bends_count as total_bends_count,
        surface_coating_count.parts_count as surface_coating_parts_count,
        insert_operations_count.parts_count as insert_operations_parts_count,
        ral_code_list.ral_code_list,
        ral_finish_list.ral_finish_list,
        surface_finish_list.surface_finish_list,
        secondary_surface_finish_list.secondary_surface_finish_list

    from source_data source_data 

    left join parts_count parts_count on
        source_data.order_id = parts_count.order_id

    left join cnc_count cnc_count on 
        source_data.order_id = cnc_count.order_id
        
    left join laser_count laser_count on 
        source_data.order_id = laser_count.order_id
    
    left join bending_parts bending_parts on 
        source_data.order_id = bending_parts.order_id
            
    left join surface_coating_count surface_coating_count on 
        source_data.order_id = surface_coating_count.order_id
            
    left join insert_operations_count insert_operations_count on 
        source_data.order_id = insert_operations_count.order_id
            
    left join ral_code_list ral_code_list on 
        source_data.order_id = ral_code_list.order_id
            
    left join ral_finish_list ral_finish_list on 
        source_data.order_id = ral_finish_list.order_id
            
    left join surface_finish_list surface_finish_list on 
        source_data.order_id = surface_finish_list.order_id
            
    left join secondary_surface_finish_list secondary_surface_finish_list on 
        source_data.order_id = secondary_surface_finish_list.order_id
)
select * from final