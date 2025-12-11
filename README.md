# Iceberg Explorer

![Iceberg Explorer](explorer.png)

**Deployed Application**: [https://iceberg-explore.uc.r.appspot.com/](https://iceberg-explore.uc.r.appspot.com/)

A comprehensive, user-friendly web interface for exploring and analyzing Apache Iceberg tables stored in Google Cloud Storage (GCS). Browse buckets, analyze table metadata, view sample data, and visualize table architecture with pixel-perfect diagrams.

## Features

- 🔍 **GCS Bucket Browser**: Navigate through GCS buckets and folders with project selection
- 📊 **Table Analysis**: Comprehensive analysis of Iceberg table metadata, schema, and partitions
- 🎨 **Architecture Visualization**: Custom SVG-based interactive diagrams showing the hierarchy of Metadata, Snapshots, Manifest Lists, Manifests, and Data Files
- 📈 **Visualizations**: Interactive charts for partitions, file sizes, and statistics
- 📋 **Sample Data**: View sample rows from your Iceberg tables
- 🔄 **Snapshot Comparison**: Compare snapshots to see what changed (like GitHub diff)
- 🎯 **Table Discovery**: Automatically discover all Iceberg tables in a bucket
-  **Responsive UI**: Modern, user-friendly interface that works on all devices
- 🔐 **Secure Authentication**: Google Sign-In integration with profile management
- ☁️ **GCS Integration**: Seamless connection to Google Cloud Storage

> **Note**: BigQuery Iceberg search support is implemented in the backend but currently disabled in the UI.

## Usage Walkthrough

### 1. Select a Project
Upon logging in, use the project selector in the top-left corner of the Sidebar to choose the Google Cloud Project you want to explore. You can select from the dropdown list or enter a Project ID manually.

### 2. Browse Storage
The **Storage** tab allows you to navigate your GCS buckets.
- Click on a bucket to expand it.
- Navigate through folders to find your Iceberg tables.
- Iceberg tables are automatically detected and marked with an icon.
- Click on a table to load its metadata and visualization.

### 3. Explore Table Details
Once a table is selected, the main view provides several tabs:
- **Graph**: A hierarchical visualization of the table's structure (Snapshots -> Manifest Lists -> Manifests -> Data Files).
- **Metadata**: Detailed JSON view of the table's metadata, including schema, partition specs, and properties.
- **Stats**: Visual charts showing file size distribution, partition counts, and other statistics.
- **Partitions**: A list of all partitions in the table.
- **Snapshots**: A history of table snapshots. Select two snapshots to compare them.
- **Sample Data**: Preview actual data rows from the table.

### 4. Compare Snapshots
In the **Snapshots** tab:
1. Select a "Base Snapshot" (the older version).
2. Select a "Comparison Snapshot" (the newer version).
3. Click "Compare" to see a diff of added, removed, and modified files, along with size and record count deltas.

## Prerequisites

- **Node.js** 22+ and npm
- **Python** 3.9+ (Python 3.11+ recommended)
- **Google Cloud SDK** (gcloud CLI) - for authentication
- **Access to GCS buckets** containing Iceberg tables
- **Google Cloud Project** with appropriate permissions:
  - Storage Object Viewer/Admin
  - Resource Manager Viewer (for project listing)
  - BigQuery Data Viewer (for BigQuery search features)

## Local Development Setup

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

### 6. Configure Environment Variables

Create a `.env.local` file in the root directory to configure your local environment:

```bash
# Create .env.local
touch .env.local
```

Add the following variables to `.env.local`:

```env
# Backend URL for local development
NEXT_PUBLIC_API_URL=http://localhost:8000

# Optional: Google OAuth credentials for local testing
# If not provided, you can use "Dev Login" (No Auth) in development mode
GOOGLE_CLIENT_ID=your_client_id
GOOGLE_CLIENT_SECRET=your_client_secret
NEXTAUTH_URL=http://localhost:3000
NEXTAUTH_SECRET=your_random_secret
```

> **Note**: In development mode (`npm run dev`), a **"Dev Login"** button will appear on the login page. This allows you to sign in without Google credentials, using your local Application Default Credentials (ADC) for backend access.

### 7. Start the Application (Development Mode)

The recommended way to start the application locally is using the `start.sh` script. This script handles:
- Checking for required environment variables
- Managing port conflicts (automatically kills processes on port 8000)
- Starting both Backend (Python/FastAPI) and Frontend (Next.js) servers
- Graceful shutdown of both servers

```bash
# Make sure you're in the project root
chmod +x start.sh
./start.sh
```

This will launch:
- **Frontend**: http://localhost:3000
- **Backend**: http://localhost:8000 (with hot-reloading)

#### Alternative: Manual Start

If you prefer to run services separately:

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

### 8. Access the Application

- **Frontend**: http://localhost:3000
- **Backend API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs (Swagger UI)

## Deployment

### Local Production Deployment

To run the application locally in a production-like environment (optimized build):

1. **Build the Frontend**:
   ```bash
   npm run build
   ```

2. **Start the Frontend**:
   ```bash
   npm start
   ```

3. **Start the Backend (Production Mode)**:
   ```bash
   source .venv/bin/activate
   cd backend
   # Run without --reload for production performance
   python -m uvicorn main:app --host 0.0.0.0 --port 8000
   ```

### Cloud Deployment (App Engine)

The application is deployed as **two separate App Engine services** to support different runtimes for the frontend (Node.js 22) and backend (Python 3.11), while maintaining a unified domain.

1. **Architecture**:
   - **Frontend Service (`default`)**: Next.js application running on Node.js 22. Handles UI and API proxying.
   - **Backend Service (`backend`)**: FastAPI application running on Python 3.11. Handles business logic and GCS operations.
   - **Routing**: `dispatch.yaml` routes all traffic matching `/api/backend/*` to the `backend` service.

2. **Configuration Files**:
   - `app.yaml`: Frontend service configuration.
   - `backend/app.yaml`: Backend service configuration.
   - `dispatch.yaml`: Routing rules.
   - `.github/workflows/deploy.yml`: CI/CD workflow that deploys both services and the dispatch configuration.

3. **Secrets**:
   Configure the following secrets in your GitHub Repository:
   - `GCP_PROJECT_ID`: Your Google Cloud Project ID
   - `WIF_PROVIDER`: Workload Identity Federation Provider
   - `WIF_SERVICE_ACCOUNT`: Service Account for deployment
   - `NEXT_PUBLIC_API_URL`: URL of your deployed backend (e.g., `https://backend-dot-YOUR_PROJECT_ID.uc.r.appspot.com` or just `/api/backend` if using dispatch)
   - `NEXTAUTH_URL`: URL of your deployed frontend (e.g., `https://YOUR_PROJECT_ID.uc.r.appspot.com`)
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
