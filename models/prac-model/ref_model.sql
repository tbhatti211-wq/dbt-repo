with employee_address AS(
    SELECT 
        emp_firstname,
        emp_lastname,
        emp_street,
        emp_city,
        emp_country
    from {{ ref('employee') }}
)

select * from employee_address