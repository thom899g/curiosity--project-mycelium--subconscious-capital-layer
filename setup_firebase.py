"""
Autonomous Firebase setup for Mycelium Network
Handles project creation, service account generation, and configuration
"""

import json
import os
import subprocess
from pathlib import Path
import logging
from typing import Optional, Dict
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FirebaseAutonomousSetup:
    """Autonomous Firebase project setup and configuration"""
    
    def __init__(self):
        self.secrets_dir = Path("secrets")
        self.secrets_dir.mkdir