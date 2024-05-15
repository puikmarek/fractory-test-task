with source_data as (
    select
        order_part_id,
        order_id,
        selected_process_type,
        material_name,
        material_type,
        weight_g,
        quantity,
        manufacturer_price_eur,
        has_bending,
        has_surface_coating,
        has_insert_operations,
        bends_count,
        created_at
    from analytics_engineer_test_task.parts 
),

surface_finish as (
    select
        order_part_id,
        JSON_EXTRACT_SCALAR(process_config, '$.value') as surface_finish
    from analytics_engineer_test_task.parts_surface_finish_config
    where process_name = 'SURFACE_FINISH'
),

secondary_surface_finish as (
    select
        order_part_id,
        JSON_EXTRACT_SCALAR(process_config, '$.value') as secondary_surface_finish
    from analytics_engineer_test_task.parts_surface_finish_config
    where process_name = 'SECONDARY_SURFACE_FINISH'
),

ral as (
    select
        order_part_id,
        JSON_EXTRACT_SCALAR(process_config, '$.ralCode') as ral_code,
        JSON_EXTRACT_SCALAR(process_config, '$.ralFinish') as ral_finish
    from analytics_engineer_test_task.parts_surface_finish_config
    where process_name = 'SECONDARY_SURFACE_FINISH_RAL'
),

final as (
    select 
        parts.order_part_id,
        parts.order_id,
        parts.selected_process_type,
        parts.material_name,
        parts.material_type,
        parts.weight_g,
        parts.quantity,
        parts.manufacturer_price_eur,
        parts.has_bending,
        parts.has_surface_coating,
        parts.has_insert_operations,
        parts.bends_count,
        parts.created_at,
        sf.surface_finish,
        ssf.secondary_surface_finish,
        ral.ral_code,
        ral.ral_finish
    from source_data parts

    left join surface_finish sf on
        sf.order_part_id = parts.order_part_id

    left join secondary_surface_finish ssf on
        ssf.order_part_id = parts.order_part_id

    left join ral ral on
        ral.order_part_id = parts.order_part_id
)

select * from final
