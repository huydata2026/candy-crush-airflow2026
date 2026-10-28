# candy-crush-airflow

Pipeline ETL dữ liệu Candy Crush Saga và kết quả xổ số miền Bắc, chạy trên Airflow 3.3.2, ghi vào BigQuery. Repo đi kèm CI/CD trên GitHub Actions: mọi thay đổi phải qua kiểm tra trước khi merge, merge xong tự deploy lên VM.

## Cấu trúc

```
.
├── dags/
│   ├── 1_..7_*.py          # các DAG
│   ├── candycrush/         # logic dùng chung cho DAG Candy Crush (config, ngày chạy, dựng SQL)
│   ├── xsmb/               # gọi API xổ số, biến đổi dữ liệu
│   ├── sql/                # file SQL và JSON danh sách event
│   └── .airflowignore      # báo Airflow bỏ qua thư mục không chứa DAG
├── tests/unit/             # unit test, chạy không cần Airflow
├── .github/workflows/      # ci.yml, cd.yml
├── docker-compose.yaml     # cụm Airflow (scheduler, worker, api-server, postgres, redis)
├── Dockerfile              # image Airflow + provider Google
├── requirements.txt        # thư viện cài vào image
├── requirements-dev.txt    # ruff, pytest, chỉ cài trên máy dev
├── pyproject.toml          # cấu hình ruff và pytest
├── .env.example            # mẫu biến môi trường, copy thành .env rồi điền
└── secrets/                # chỗ để key service account, không commit
```

## Các DAG

Tất cả chạy lúc 09:15 UTC hằng ngày, `catchup=False`, dùng chung `DEFAULT_ARGS` trong `dags/candycrush/config.py`.

| DAG | Làm gì |
|---|---|
| `1_etl_flat_event_candycrushsaga` | Làm phẳng event, 3 bước, ngày chạy cố định |
| `2_etl_flat_event_candycrushsaga_param_cach1` | Như DAG 1, nhận `run_date` khi trigger |
| `3_etl_flat_event_candycrushsaga_param_cach2` | Chạy theo khoảng `start_date` → `end_date`, tối đa 31 ngày |
| `4_full_etl_latedata_param` | Xử lý late data, mỗi lần làm lại D-1, D-2, D-3 |
| `5_etl_case_1_20_events` | Mỗi event một task (dynamic task mapping) |
| `6_etl_flat_event_candycrushsaga_operator` | Dùng `BigQueryInsertJobOperator` thay PythonOperator |
| `7_etl_data_xsmb` | Lấy kết quả xổ số từ API, ghi vào BigQuery, cần `XSMB_API_KEY` |

## Chạy trên máy

```bash
cp .env.example .env        # điền các dòng trống theo hướng dẫn trong file
# chép key service account vào secrets/gcp-sa.json
docker compose up airflow-init
docker compose up -d
```

Airflow UI ở http://localhost:8080, đăng nhập bằng tài khoản admin trong `.env`.

Kiểm tra code trước khi push:

```bash
pip install -r requirements-dev.txt
ruff check .
pytest tests/unit
```

## CI/CD

**CI** (`ci.yml`): chạy trên mỗi Pull Request vào `main`. Lint bằng ruff và chạy unit test. Đỏ thì không merge.

**CD** (`cd.yml`): chạy khi `main` có commit mới. GitHub Actions SSH vào VM, `git pull` rồi `docker compose up -d --build`. Thư mục `dags/` được mount vào container nên DAG mới có mặt ngay, chỉ build lại image khi đổi `requirements.txt` hoặc `Dockerfile`.

Secret cần khai báo trong Settings → Secrets and variables → Actions:

| Tên | Giá trị |
|---|---|
| `VM_HOST` | IP của VM |
| `VM_USER` | user SSH trên VM |
| `VM_SSH_KEY` | private key dành riêng cho GitHub Actions |

`.env` và `secrets/gcp-sa.json` không bao giờ lên GitHub, tạo tay trên VM một lần.
