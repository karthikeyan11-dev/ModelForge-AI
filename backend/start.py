"""
Quick Start Script for AI Data Cleaning System
This script helps you set up and run the application
"""

import os
import sys
import subprocess
import time
from typing import List

# Ensure all backend modules are resolvable regardless of how script is invoked
# Path stabilization for Docker and local dev after move to backend/ folder
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def print_banner():
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║  🚀 Prism AI - Professional AutoML Platform             ║
    ╠════════════════════════════════════════════════════════════╣
    ║  This script will help you get started quickly            ║
    ╚════════════════════════════════════════════════════════════╝
    """)

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8 or higher is required")
        print(f"   Current version: {version.major}.{version.minor}.{version.micro}")
        return False
    print(f"✅ Python version: {version.major}.{version.minor}.{version.micro}")
    return True

def check_env_file():
    """Check if .env file exists"""
    if os.path.exists('.env'):
        print("✅ .env file found")
        return True
    else:
        print("❌ .env file not found")
        print("   Please create .env file with your Euri API key")
        return False

def install_requirements():
    """Install required packages"""
    print("\n📦 Installing requirements...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install requirements")
        return False

def main():
    print_banner()
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Check .env file
    if not check_env_file():
        sys.exit(1)
    
    # Ask user what to do
    print("\n" + "="*60)
    print("What would you like to do?")
    print("1. Install requirements")
    print("2. Start FastAPI backend")
    print("3. Test Euri API connection")
    print("4. Exit")
    print("="*60)
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == "1":
        install_requirements()
    elif choice == "2":
        print("\n🚀 Starting FastAPI backend...")
        print("   Access API docs at: http://127.0.0.1:8000/docs")
        subprocess.run([sys.executable, "scripts/backend.py"])
    elif choice == "3":
        print("\n🔍 Testing Euri API connection...")
        subprocess.run([sys.executable, "scripts/euri_client.py"])
    elif choice == "4":
        print("\n👋 Goodbye!")
        sys.exit(0)
    else:
        print("\n❌ Invalid choice")

if __name__ == "__main__":
    main()
