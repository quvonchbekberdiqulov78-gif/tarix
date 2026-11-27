import os
from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL")
pool = ConnectionPool(DATABASE_URL, kwargs={"row_factory": dict_row}, open=False) if DATABASE_URL else None

@asynccontextmanager
async def lifespan(app):
    if pool:
        pool.open()
        pool.wait()
    yield
    if pool:
        pool.close()

app = FastAPI(title="TA'LIM NAZORATI API", version="0.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def db():
    if pool is None:
        raise HTTPException(503, "DATABASE_URL sozlanmagan")
    return pool.connection()

class StudentCreate(BaseModel):
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)
    phone: str | None = None
    parent_phone: str | None = None
    group_id: UUID | None = None
    subject_id: UUID | None = None
    monthly_fee: Decimal = Field(default=Decimal("0"), ge=0)

class AttendanceCreate(BaseModel):
    student_id: UUID
    lesson_date: date
    status: Literal["present", "absent", "late", "excused"]

class PaymentCreate(BaseModel):
    student_id: UUID
    amount: Decimal = Field(gt=0)
    period_month: date
    paid_on: date = Field(default_factory=date.today)
    note: str | None = None

@app.get("/")
def root():
    return {"name":"TA'LIM NAZORATI","status":"ok","version":"0.1.0"}

@app.get("/health")
def health():
    if pool is None:
        return {"status":"degraded","database":"not_configured"}
    try:
        with db() as conn:
            conn.execute("SELECT 1")
        return {"status":"ok","database":"connected"}
    except Exception as exc:
        raise HTTPException(503,"Database ulanishida xatolik") from exc

@app.get("/dashboard")
def dashboard():
    with db() as conn:
        return conn.execute("""SELECT
        (SELECT COUNT(*) FROM students WHERE is_active) AS total_students,
        (SELECT COUNT(*) FROM tests) AS total_tests,
        (SELECT COALESCE(ROUND(AVG(score_percent),1),0) FROM test_attempts) AS average_result,
        (SELECT COUNT(*) FROM attendance WHERE lesson_date=CURRENT_DATE AND status IN ('present','late')) AS present_today,
        (SELECT COUNT(*) FROM homework_results WHERE status='done') AS completed_homework""").fetchone()

@app.get("/students")
def students(q: str | None = None):
    with db() as conn:
        return conn.execute("""SELECT st.*,g.name AS group_name,s.name AS subject_name
        FROM students st LEFT JOIN student_groups g ON g.id=st.group_id
        LEFT JOIN subjects s ON s.id=st.subject_id
        WHERE st.is_active AND (%s IS NULL OR (st.first_name || ' ' || st.last_name) ILIKE %s)
        ORDER BY st.last_name,st.first_name""",(q, f"%{q}%" if q else None)).fetchall()

@app.post("/students",status_code=201)
def create_student(x: StudentCreate):
    with db() as conn:
        return conn.execute("""INSERT INTO students(first_name,last_name,phone,parent_phone,group_id,subject_id,monthly_fee)
        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *""",(x.first_name,x.last_name,x.phone,x.parent_phone,x.group_id,x.subject_id,x.monthly_fee)).fetchone()

@app.get("/groups")
def groups():
    with db() as conn:
        return conn.execute("""SELECT g.id,g.name,g.schedule,s.name AS subject_name,u.full_name AS teacher_name,
        COUNT(st.id)::int AS student_count FROM student_groups g
        LEFT JOIN subjects s ON s.id=g.subject_id LEFT JOIN app_users u ON u.id=g.teacher_id
        LEFT JOIN students st ON st.group_id=g.id AND st.is_active
        GROUP BY g.id,s.name,u.full_name ORDER BY g.name""").fetchall()

@app.post("/attendance",status_code=201)
def attendance(x: AttendanceCreate):
    with db() as conn:
        return conn.execute("""INSERT INTO attendance(student_id,lesson_date,status) VALUES (%s,%s,%s)
        ON CONFLICT(student_id,lesson_date) DO UPDATE SET status=EXCLUDED.status RETURNING *""",
        (x.student_id,x.lesson_date,x.status)).fetchone()

@app.get("/attendance/monthly")
def attendance_month(month: date = Query(...)):
    with db() as conn:
        return conn.execute("""SELECT st.id,st.first_name,st.last_name,COUNT(a.id)::int AS lessons,
        COUNT(*) FILTER (WHERE a.status IN ('present','late'))::int AS attended,
        COUNT(*) FILTER (WHERE a.status='absent')::int AS absent,
        COUNT(*) FILTER (WHERE a.status='late')::int AS late,
        CASE WHEN COUNT(a.id)=0 THEN 0 ELSE ROUND(100.0*COUNT(*) FILTER (WHERE a.status IN ('present','late'))/COUNT(a.id),1) END AS attendance_percent
        FROM students st LEFT JOIN attendance a ON a.student_id=st.id
        AND date_trunc('month',a.lesson_date)=date_trunc('month',%s::date)
        WHERE st.is_active GROUP BY st.id ORDER BY st.last_name""",(month,)).fetchall()

@app.post("/payments",status_code=201)
def payment(x: PaymentCreate):
    with db() as conn:
        return conn.execute("""INSERT INTO payments(student_id,amount,paid_on,period_month,note)
        VALUES (%s,%s,%s,%s,%s) RETURNING *""",(x.student_id,x.amount,x.paid_on,x.period_month,x.note)).fetchone()

@app.get("/payments/debts")
def debts(month: date = Query(...)):
    with db() as conn:
        return conn.execute("""SELECT st.id,st.first_name,st.last_name,st.monthly_fee,
        COALESCE(SUM(p.amount),0) AS paid,
        GREATEST(st.monthly_fee-COALESCE(SUM(p.amount),0),0) AS debt
        FROM students st LEFT JOIN payments p ON p.student_id=st.id
        AND date_trunc('month',p.period_month)=date_trunc('month',%s::date)
        WHERE st.is_active GROUP BY st.id ORDER BY debt DESC""",(month,)).fetchall()

@app.get("/tests")
def tests():
    with db() as conn:
        return conn.execute("SELECT id,title,duration_minutes,created_at FROM tests ORDER BY created_at DESC").fetchall()

@app.get("/students/{student_id}/profile")
def profile(student_id: UUID):
    with db() as conn:
        student=conn.execute("SELECT * FROM students WHERE id=%s",(student_id,)).fetchone()
        if not student:
            raise HTTPException(404,"O'quvchi topilmadi")
        att=conn.execute("SELECT lesson_date,status FROM attendance WHERE student_id=%s ORDER BY lesson_date DESC",(student_id,)).fetchall()
        attempts=conn.execute("SELECT id,test_id,correct_count,wrong_count,score_percent,source,completed_at FROM test_attempts WHERE student_id=%s ORDER BY created_at DESC",(student_id,)).fetchall()
        pays=conn.execute("SELECT amount,paid_on,period_month,note FROM payments WHERE student_id=%s ORDER BY paid_on DESC",(student_id,)).fetchall()
        return {"student":student,"attendance":att,"tests":attempts,"payments":pays}
