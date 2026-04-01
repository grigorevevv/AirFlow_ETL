-- Инициализационный скрипт для PostgreSQL для учебных упражнений

-- Создание таблицы студентов
CREATE TABLE IF NOT EXISTS students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    enrollment_date DATE,
    grade INTEGER
);

-- Создание таблицы курсов
CREATE TABLE IF NOT EXISTS courses (
    course_id SERIAL PRIMARY KEY,
    course_name VARCHAR(100),
    instructor VARCHAR(100),
    credits INTEGER
);

-- Создание таблицы зачислений
CREATE TABLE IF NOT EXISTS enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES students(student_id),
    course_id INTEGER REFERENCES courses(course_id),
    enrollment_date DATE,
    grade CHAR(1)
);

-- Вставка тестовых данных
INSERT INTO students (first_name, last_name, email, enrollment_date, grade) VALUES
('Alice', 'Johnson', 'alice@example.com', '2023-09-01', 85),
('Bob', 'Smith', 'bob@example.com', '2023-09-01', 92),
('Charlie', 'Brown', 'charlie@example.com', '2023-09-01', 78),
('Diana', 'Prince', 'diana@example.com', '2023-09-01', 96),
('Eve', 'Wilson', 'eve@example.com', '2023-09-01', 8)
ON CONFLICT DO NOTHING;

INSERT INTO courses (course_name, instructor, credits) VALUES
('Introduction to Data Science', 'Dr. Smith', 3),
('Python Programming', 'Dr. Johnson', 4),
('Database Systems', 'Dr. Williams', 3),
('Machine Learning', 'Dr. Brown', 4)
ON CONFLICT DO NOTHING;

INSERT INTO enrollments (student_id, course_id, enrollment_date, grade) VALUES
(1, 1, '2023-09-01', 'A'),
(1, 2, '2023-09-01', 'B'),
(2, 1, '2023-09-01', 'A'),
(2, 3, '2023-09-01', 'A'),
(3, 2, '2023-09-01', 'B'),
(4, 4, '2023-09-01', 'A'),
(5, 1, '2023-09-01', 'A')
ON CONFLICT DO NOTHING;

-- Создание индексов для улучшения производительности
CREATE INDEX IF NOT EXISTS idx_students_email ON students(email);
CREATE INDEX IF NOT EXISTS idx_enrollments_student ON enrollments(student_id);
CREATE INDEX IF NOT EXISTS idx_enrollments_course ON enrollments(course_id);