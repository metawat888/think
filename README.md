# ClassBoard — กระดานไวท์บอร์ดแสดงความคิดเห็นนักเรียน (GitHub & Vercel Ready)

เว็บแอปพลิเคชันกระดานไวท์บอร์ดแสดงความคิดเห็นสำหรับห้องเรียน ที่ให้นักเรียนพิมพ์ชื่อเล่นแล้วเข้ามาโพสต์ แปะโน้ต วาดรูป กดถูกใจ และคอมเมนต์ตอบกลับใต้โพสต์เหมือน Facebook บนกระดานไวท์บอร์ด พร้อมระบบเก็บก้อน Data (Persistent JSON Data + LocalStorage + Cloud Sync)

## วิธีอัปโหลดขึ้น GitHub และ Deploy บน Vercel.com

1. อัปโหลดไฟล์ทั้งหมดในโฟลเดอร์นี้ (`index.html`, `vercel.json`, `package.json`, `README.md`) ขึ้น GitHub Repository ของคุณ:
   ```bash
   git init
   git add .
   git commit -m "Initial commit: ClassBoard Student Whiteboard"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
   git push -u origin main
   ```
2. ไปที่ [https://vercel.com](https://vercel.com) กด **Add New... -> Project**
3. เลือก GitHub Repository ที่เพิ่งสร้าง แล้วกดปุ่ม **Deploy** ได้ทันที
