# Iceberg Explorer

![Iceberg Explorer](explorer.png)

A comprehensive, user-friendly web interface for exploring and analyzing Apache Iceberg tables stored in Google Cloud Storage (GCS). Browse buckets, analyze table metadata, view sample data, and visualize table architecture with pixel-perfect diagrams.

## Features

- 🔍 **GCS Bucket Browser**: Navigate through GCS buckets and folders with project selection
- 📊 **Table Analysis**: Comprehensive analysis of Iceberg table metadata, schema, and partitions
- 🎨 **Architecture Visualization**: Custom SVG-based interactive diagrams showing the hierarchy of Metadata, Snapshots, Manifest Lists, Manifests, and Data Files
- 📈 **Visualizations**: Interactive charts for partitions, file sizes, and statistics
- 📋 **Sample Data**: View sample rows from your Iceberg tables
- 🔄 **Snapshot Comparison**: Compare snapshots to see what changed (like GitHub diff)
- 🎯 **Table Discovery**: Automatically discover all Iceberg tables in a bucket
- 📱 **Responsive UI**: Modern, user-friendly interface that works on all devices
- 🔐 **Secure Authentication**: Google Sign-In integration with profile management
- ☁️ **GCS Integration**: Seamless connection to Google Cloud Storage

## Prerequisites

- **Node.js** 22+ and npm
- **Python** 3.9+ (Python 3.11+ recommended)
- **Google Cloud SDK** (gcloud CLI) - for authentication
- **Access to GCS buckets** containing Iceberg tables
- **Google Cloud Project** with appropriate permissions:
  - Storage Object Viewer/Admin
  - Resource Manager Viewer (for project listing)

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd iceberg_explorer
```

### 2. Authenticate with Google Cloud

You have two options for authentication:

#### Option A: Using gcloud CLI (Recommended)

```bash
# Step 1: Login to Google Cloud (for gcloud CLI)
gcloud auth login

# Step 2: Set up Application Default Credentials (REQUIRED for Python apps)
# This is different from gcloud auth login and is needed for the application
gcloud auth application-default login

# Step 3: Set your default project (optional but recommended)
gcloud config set project YOUR_PROJECT_ID

# Step 4: Set quota project for ADC (prevents billing/quota warnings)
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

**Important**: `gcloud auth login` only sets credentials for the `gcloud` CLI. For Python applications, you **must** also run `gcloud auth application-default login` to set up Application Default Credentials.

#### Option B: Using Service Account JSON Key

```bash
# Download your service account JSON key from Google Cloud Console
# Then set the environment variable:
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

**Note**: If you use `gcloud auth application-default login`, you don't need to set `GOOGLE_APPLICATION_CREDENTIALS`. The application will use Application Default Credentials automatically.

### 3. Set Up Python Virtual Environment

```bash
# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
# .venv\Scripts\activate
```

### 4. Install Backend Dependencies

```bash
# Make sure virtual environment is activated
# Upgrade pip first
pip install --upgrade pip

# Install all Python dependencies
pip install -r backend/requirements.txt
```

**Backend Dependencies:**
- FastAPI - Web framework
- PyIceberg - Iceberg table parsing
- Google Cloud Storage SDK - GCS access
- fastavro - Avro file parsing
- pyarrow & pandas - Parquet file reading
- And more (see `backend/requirements.txt`)

### 5. Install Frontend Dependencies

```bash
# Install Node.js dependencies
npm install
```

**Frontend Dependencies:**
- Next.js 14 - React framework
- TypeScript - Type safety
- Tailwind CSS - Styling
- Recharts - Data visualization
- Axios - HTTP client
- Lucide React - Icons

### 6. Start the Application

#### Option A: Using the Start Script (Recommended)

```bash
# Make sure you're in the project root
# The script will use the virtual environment automatically
chmod +x start.sh
./start.sh
```

This will start both the backend (port 8000) and frontend (port 3000) servers.

#### Option B: Manual Start

**Terminal 1 - Backend:**
```bash
# Activate virtual environment
source .venv/bin/activate

# Start backend server
cd backend
python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

**Terminal 2 - Frontend:**
```bash
# Start frontend server
npm run dev
```

### 7. Access the Application

- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs (Swagger UI)

## Deployment

### App Engine (Standard Environment)

The application is configured for deployment to Google App Engine (Node.js 22 runtime).

1. **Prerequisites**:
   - Google Cloud Project with App Engine enabled
   - GitHub Repository with this code

2. **Configuration**:
   - `app.yaml`: Configures the App Engine runtime and scaling settings.
   - `.github/workflows/deploy.yml`: GitHub Actions workflow for automated deployment.

3. **Secrets**:
   Configure the following secrets in your GitHub Repository:
   - `GCP_PROJECT_ID`: Your Google Cloud Project ID
   - `WIF_PROVIDER`: Workload Identity Federation Provider
   - `WIF_SERVICE_ACCOUNT`: Service Account for deployment
   - `NEXT_PUBLIC_API_URL`: URL of your deployed backend
   - `NEXTAUTH_URL`: URL of your deployed frontend
   - `NEXTAUTH_SECRET`: Random string for NextAuth
   - `GOOGLE_CLIENT_ID`: Google OAuth Client ID
   - `GOOGLE_CLIENT_SECRET`: Google OAuth Client Secret

## Project Structure

```
iceberg_explorer/
├── app/                          # Next.js app directory
│   ├── page.tsx                  # Main page component
│   ├── login/                    # Login page
│   ├── api/                      # API proxy routes
│   └── ...
├── components/                    # React components
│   ├── ProfileButton.tsx         # User profile & logout
│   ├── IcebergTree.tsx           # Custom SVG architecture visualization
│   └── ...
├── backend/                       # Python FastAPI backend
│   ├── app/
│   │   ├── core/                 # Core functionality (security, config)
│   │   ├── routers/              # API routers (projects, buckets, analyze)
│   │   └── services/             # Business logic (gcs, iceberg)
│   ├── main.py                   # Application entry point
│   └── requirements.txt          # Python dependencies
├── .github/workflows/            # CI/CD workflows
├── .venv/                         # Python virtual environment (gitignored)
├── start.sh                      # Startup script
├── package.json                  # Node.js dependencies
├── app.yaml                      # App Engine configuration
└── README.md                     # This file
```

## Technologies

### Frontend
- **Next.js 14** - React framework with App Router
- **TypeScript** - Type safety
- **Tailwind CSS** - Utility-first CSS framework
- **Recharts** - Composable charting library
- **Axios** - Promise-based HTTP client
- **Lucide React** - Beautiful icon library
- **Custom SVG** - Bespoke visualization engine for Iceberg trees

### Backend
- **FastAPI** - Modern, fast web framework
- **PyIceberg** - Apache Iceberg Python library
- **Google Cloud Storage SDK** - GCS integration
- **fastavro** - Fast Avro file reader
- **pyarrow** - Apache Arrow Python bindings
- **pandas** - Data manipulation library
- **uvicorn** - ASGI server

## Troubleshooting

### Authentication Issues

**Problem**: "Permission denied" or "Authentication failed"

**Common Cause**: Running `gcloud auth login` alone is not enough! This only sets credentials for the `gcloud` CLI, not for Python applications.

**Solutions**:
1. **Run Application Default Credentials setup** (REQUIRED):
   ```bash
   gcloud auth application-default login
   gcloud auth application-default set-quota-project YOUR_PROJECT_ID
   ```
2. Verify credentials are set:
   ```bash
   # Check if ADC file exists
   ls ~/.config/gcloud/application_default_credentials.json
   ```

### Backend Won't Start

**Problem**: "Address already in use" or port 8000 is occupied

**Solutions**:
1. **Use the start script** - It automatically handles port conflicts:
   ```bash
   ./start.sh
   ```

2. **Manually kill the process**:
   ```bash
   # Find what's using port 8000
   lsof -i :8000
   # Kill it (replace PID with the actual process ID)
   kill -9 <PID>
   ```

## License

MIT License - see LICENSE file for details

---

**Happy Exploring! 🚀**
