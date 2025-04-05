DROP TABLE job_descriptions CASCADE;

CREATE TABLE job_descriptions (
    id SERIAL PRIMARY KEY,
    job_id BIGINT UNIQUE NOT NULL,
    experience TEXT,
    qualifications TEXT,
    salary_range TEXT,
    location TEXT,
    country TEXT,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    work_type TEXT,
    company_size INTEGER,
    job_posting_date DATE,
    preference TEXT,
    contact_person TEXT,
    contact TEXT,
    job_title TEXT NOT NULL,
    role TEXT,
    job_portal TEXT,
    job_description TEXT,
    benefits TEXT,
    skills TEXT,
    responsibilities TEXT,
    company_name TEXT,
    company_profile TEXT,
    
    -- Additional constraints
    CHECK (latitude BETWEEN -90 AND 90),
    CHECK (longitude BETWEEN -180 AND 180),
    CHECK (company_size > 0),
    CONSTRAINT job_descriptions_valid_date CHECK (job_posting_date <= CURRENT_DATE)
);