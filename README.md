# ERP Sample (local-first, 0&#8363;)

Một app mẫu full-stack để kiểm chứng kiến trúc ERP, **chạy 100% trên máy bạn**:

- **API**: Node.js + Express (TypeScript, chạy qua `tsx`)
- **DB**: PostgreSQL **portable** (binaries, không cài service, không cần quyền admin) — tương đương Cloud SQL về mặt code
- **Web**: 1 trang tĩnh gọi API (quản lý kho hàng đơn giản)

> Tất cả nằm trong Always-Free khi sau này đưa lên GCP. Bản local này không tốn đồng nào.

## Cấu trúc

```
erp-sample/
  src/server.ts      # Express API: /health, CRUD /api/items
  src/db.ts          # Kết nối Postgres (pg Pool)
  web/index.html     # Giao diện
  db/schema.sql      # Bảng items + seed
  scripts/migrate.mjs# Tạo DB + áp schema
  scripts/pg-*.ps1   # Bật/tắt Postgres portable
  .pgsql/            # Binaries Postgres (tự tải, đã .gitignore)
  .pgdata/           # Dữ liệu Postgres (đã .gitignore)
```

## Chạy lần đầu (đã được tự động hoá khi cài)

```powershell
npm install
npm run pg:start      # bật Postgres portable (cổng 5432)
npm run db:migrate    # tạo DB erp_sample + bảng + seed
npm run dev           # chạy API + web ở http://localhost:8080
```

## Hằng ngày

```powershell
npm run pg:start      # bật DB
npm run dev           # chạy app
# ... làm việc ...
npm run pg:stop       # tắt DB khi xong (giải phóng RAM)
```

Mở: http://localhost:8080

## API

| Method | Đường dẫn          | Mô tả               |
|--------|--------------------|---------------------|
| GET    | `/health`          | Kiểm tra API + DB   |
| GET    | `/api/items`       | Danh sách mặt hàng  |
| POST   | `/api/items`       | Thêm (sku, name, quantity, unit_price) |
| DELETE | `/api/items/:id`   | Xóa theo id         |

## Khi nào cần lên GCP?

Chỉ khi cần URL public cho người khác xem. Lúc đó:
- API → **Cloud Run** (Always-Free, scale về 0)
- DB → giữ Postgres: dùng **Cloud SQL** (tốn ~$8–10/tháng) **hoặc** Postgres serverless free tier ngoài GCP

Code gần như giữ nguyên — chỉ đổi `DATABASE_URL` trong biến môi trường.

## Lưu ý bảo mật

DB local dùng `trust auth` (không mật khẩu) cho tiện dev. **Không bao giờ** dùng cấu hình này khi lên production.
