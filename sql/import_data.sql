COPY job_descriptions (
    job_id,
    experience,
    qualifications,
    salary_range,
    location,
    country,
    latitude,
    longitude,
    work_type,
    company_size,
    job_posting_date,
    preference,
    contact_person,
    contact,
    job_title,
    role,
    job_portal,
    job_description,
    benefits,
    skills,
    responsibilities,
    company_name,
    company_profile
)
FROM STDIN
WITH CSV 
     HEADER 
     DELIMITER ',' 
     NULL 'null';
