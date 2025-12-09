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
- 🔐 **GCS Integration**: Seamless connection to Google Cloud Storage

## Prerequisites

- **Node.js** 18+ and npm
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

## Usage Guide

### 1. Select a GCP Project

- On the main page, you'll see a project selector
- Choose a project from the dropdown, or enter a project ID manually
- Buckets will automatically refresh based on the selected project

### 2. Browse GCS Buckets

- Select a bucket from the list
- Navigate through folders by clicking on them
- Iceberg tables are automatically detected and marked with a special icon

### 3. Discover Iceberg Tables

- Click the **"Discover Iceberg Tables"** button to scan the entire bucket
- The system will find all `*.metadata.json` files and list discovered tables
- Click on any discovered table to analyze it

### 4. Analyze a Table

Click on an Iceberg table to view detailed analysis with multiple tabs:

#### Overview Tab
- Table metadata (format version, location, UUID)
- Current snapshot information
- Table properties

#### Architecture Tab
- **Interactive Graph**: Visualizes the relationship between Metadata, Snapshots, Manifest Lists, Manifests, and Data Files
- **Custom SVG Layout**: Pixel-perfect representation of the Iceberg spec
- **Zoom & Pan**: Explore large table structures easily

#### Schema Tab
- Complete schema with field types, IDs, and documentation
- Field hierarchy and relationships

#### Partitions Tab
- Partition specification details
- Partition statistics with visualizations
- File distribution across partitions

#### Statistics Tab
- Overall table statistics (total files, records, size)
- Per-snapshot statistics with delta information
- Visual charts and graphs

#### Sample Data Tab
- View sample rows from the table
- Select a specific snapshot to view data from that point in time
- Configurable row limit (default: 100 rows)

#### Snapshots Tab
- Compare any two snapshots side-by-side
- See added, removed, and modified files
- View statistics delta (like GitHub diff)
- Color-coded changes (green=added, red=removed, yellow=modified)

## API Endpoints

The backend provides the following REST API endpoints:

### Project & Bucket Management

- `GET /projects` - List all accessible GCP projects
- `GET /buckets?project_id={id}` - List buckets in a project
- `GET /browse?bucket={name}&path={path}&project_id={id}` - Browse bucket contents

### Table Analysis

- `GET /analyze?bucket={name}&path={path}&project_id={id}` - Analyze Iceberg table metadata
- `GET /analyze/snapshot?bucket={name}&path={path}&snapshot_id={id}&project_id={id}` - Get specific snapshot data
- `GET /sample?bucket={name}&path={path}&limit={n}&snapshot_id={id}&project_id={id}` - Get sample data
- `GET /snapshot/compare?bucket={name}&path={path}&snapshot_id_1={id1}&snapshot_id_2={id2}&project_id={id}` - Compare two snapshots

### Table Discovery

- `GET /discover?bucket={name}&project_id={id}` - Discover all Iceberg tables in a bucket

## Project Structure

```
iceberg_explorer/
├── app/                          # Next.js app directory
│   ├── page.tsx                  # Main page component
│   ├── layout.tsx                # Root layout
│   ├── globals.css               # Global styles
│   └── api/                      # API proxy routes
│       └── backend/[...path]/    # Backend API proxy
├── components/                    # React components
│   ├── BucketBrowser.tsx         # GCS bucket navigation
│   ├── TableAnalyzer.tsx         # Main table analysis component
│   ├── IcebergGraphView.tsx      # Graph view container
│   ├── IcebergTree.tsx           # Custom SVG architecture visualization
│   ├── MetadataView.tsx          # Table metadata display
│   ├── SchemaView.tsx            # Schema information
│   ├── PartitionView.tsx         # Partition analysis with charts
│   ├── StatsView.tsx             # Statistics and visualizations
│   ├── SampleDataView.tsx        # Sample data viewer
│   └── SnapshotComparisonView.tsx # Snapshot comparison UI
├── types/                         # TypeScript type definitions
│   └── index.ts                  # All type definitions
├── backend/                       # Python FastAPI backend
│   ├── main.py                   # API server and business logic
│   └── requirements.txt          # Python dependencies
├── .venv/                         # Python virtual environment (gitignored)
├── start.sh                      # Startup script
├── package.json                  # Node.js dependencies
├── tsconfig.json                 # TypeScript configuration
├── tailwind.config.js           # Tailwind CSS configuration
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
3. If using service account, verify `GOOGLE_APPLICATION_CREDENTIALS` is set:
   ```bash
   echo $GOOGLE_APPLICATION_CREDENTIALS
   ```
4. Check your project permissions - you need:
   - `storage.buckets.list` (to list buckets)
   - `storage.objects.get` (to read files)
   - `resourcemanager.projects.list` (to list projects)

### Backend Won't Start

**Problem**: "Address already in use" or port 8000 is occupied

**Solutions**:
1. **Use the start script** - It automatically handles port conflicts:
   ```bash
   ./start.sh
   ```
   The script will kill any existing process on port 8000.

2. **Manually kill the process**:
   ```bash
   # Find what's using port 8000
   lsof -i :8000
   # Kill it (replace PID with the actual process ID)
   kill -9 <PID>
   ```

3. **Use a different port** (if you want to keep the existing process):
   ```bash
   # Start backend on port 8001
   cd backend
   uvicorn main:app --host 0.0.0.0 --port 8001 --reload
   ```
   Then update `next.config.js` to proxy to port 8001 instead of 8000.

### Snapshot IDs Not Found

**Problem**: "Snapshot not found" errors

**Solutions**:
1. This is usually a precision issue with large integers
2. Make sure you're using the latest code (snapshot IDs are now strings)
3. Restart both backend and frontend servers

### No Data Files Found

**Problem**: Table shows 0 data files

**Solutions**:
1. Check that the table has snapshots (current-snapshot-id != -1)
2. Verify the metadata file is the latest version
3. Check backend logs for Avro parsing errors
4. Ensure `fastavro` is installed: `pip install fastavro`

## Development

### Running in Development Mode

Both servers support hot-reload:

- **Backend**: Automatically reloads on file changes (uvicorn --reload)
- **Frontend**: Next.js Fast Refresh enabled

### Adding New Features

1. **Backend**: Add endpoints in `backend/main.py`
2. **Frontend**: Add components in `components/`
3. **Types**: Update `types/index.ts` for new data structures

## License

MIT License - see LICENSE file for details

---

**Happy Exploring! 🚀**
