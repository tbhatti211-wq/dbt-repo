{{ config(materialized="table") }}


with
    employee as (
        select
            empid as emp_id,
            split_part(name, ' ', 1) as emp_firstname,
            split_part(name, ' ', 2) as emp_lastname,
            salary * 12 as emp_salary_annual,
            to_char(hiredate, '%Y-%m') as emp_hiredate
        from {{ source("employee", "EMPLOYEE_RAW") }}
    )
select *
from employee
