# TA'LIM NAZORATI

Ta'lim markazlari uchun Android va backend platformasi.

- backend/: FastAPI REST API
- mobile/: Flutter Android ilovasi
- Neon PostgreSQL: ma'lumotlar bazasi

Backend: cd backend && pip install -r requirements.txt && uvicorn app.main:app --host 0.0.0.0 --port 10000

Maxfiy DATABASE_URL qiymatini GitHub'ga yozmang; Render Environment Variables orqali kiriting. Hozirgi versiya boshlang'ich ishchi skelet bo'lib, ishlab chiqarishga chiqarishdan oldin autentifikatsiya, rol nazorati, OMR, Rasch kalibrlash va to'liq sinovlar bajarilishi shart.
