"""
Project Mycelium: Genesis Node Implementation
Core daemon for decentralized autonomous capital network.
Three-layer architecture: Sensor → Nervous → Muscle
"""

import asyncio
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass
import firebase_admin
from firebase_admin import credentials, firestore, db
from web3 import Web3
from web3.exceptions import TransactionNotFound
import ccxt
import pandas as pd
import numpy as np
import aiohttp
import os
import json
from datetime import datetime, timedelta
from prometheus_client import start_http_server, Counter, Gauge

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class NodeConfig:
    """Node configuration with paranoid defaults"""
    node_id: str
    vault_address: str
    max_capital_usd: float = 10.0
    min_profit_multiplier: float = 2.0
    max_gas_gwei: int = 50
    heartbeat_interval: int = 30
    cooldown_period: int = 60
    
class FailureModeResponse:
    """Paranoid failure mode handler"""
    
    def __init__(self, config: NodeConfig):
        self.config = config
        self.gas_history = []
        
    def handle_gas_spike(self, current_gas_gwei: int) -> Dict[str, Any]:
        """Dynamic gas ceiling with 20-block moving average"""
        self.gas_history.append(current_gas_gwei)
        if len(self.gas_history) > 20:
            self.gas_history.pop(0)
            
        if len(self.gas_history) >= 5:
            avg_gas = np.mean(self.gas_history)
            if current_gas_gwei > avg_gas * 1.5:
                logger.warning(f"Gas spike detected: {current_gas_gwei} > {avg_gas*1.5}")
                return {"action": "pause", "duration": 300, "reason": "gas_spike"}
        return {"action": "proceed"}
    
    def handle_micro_profit(self, profit_usd: float, gas_cost_usd: float) -> Dict[str, Any]:
        """Minimum profit threshold: 2x gas cost + 0.1% buffer"""
        min_profit = gas_cost_usd * self.config.min_profit_multiplier * 1.001
        if profit_usd < min_profit:
            logger.info(f"Micro profit: {profit_usd} < {min_profit}")
            return {"action": "skip", "reason": "micro_profit"}
        return {"action": "proceed"}

class MyceliumNode:
    """Autonomous capital organism node"""
    
    def __init__(self, config: NodeConfig):
        self.config = config
        self.failure_handler = FailureModeResponse(config)
        
        # Initialize metrics
        self.metrics = {
            'opportunities_scanned': Counter('mycelium_opportunities_scanned', 'Total opportunities scanned'),
            'trades_executed': Counter('mycelium_trades_executed', 'Total trades executed'),
            'profits_usd': Gauge('mycelium_profits_usd', 'Total profits in USD'),
            'node_capital': Gauge('mycelium_node_capital', 'Current node capital in USD')
        }
        
        # Initialize components
        self._init_firebase()
        self._init_web3()
        self._init_exchanges()
        
    def _init_firebase(self):
        """Initialize Firebase with autonomous fallback"""
        try:
            # Check for existing service account
            if os.path.exists('secrets/service_account.json'):
                cred = credentials.Certificate('secrets/service_account.json')
                firebase_admin.initialize_app(cred, {
                    'databaseURL': os.getenv('FIREBASE_DATABASE_URL')
                })
                logger.info("Firebase initialized from service account")
            else:
                logger.warning("No service account found. Running in offline mode.")
                self.firestore = None
                self.rtdb = None
                return
                
            self.firestore = firestore.client()
            self.rtdb = db.reference()
            
            # Register node
            self._register_node()
            
        except Exception as e:
            logger.error(f"Firebase initialization failed: {e}")
            self.firestore = None
            self.rtdb = None
    
    def _register_node(self):
        """Register node in Firebase registry"""
        if not self.firestore:
            return
            
        node_ref = self.firestore.collection('nodes').document(self.config.node_id)
        node_data = {
            'node_id': self.config.node_id,
            'trust_score': 100,
            'stake_amount': 0,
            'last_heartbeat': datetime.utcnow().isoformat(),
            'status': 'active',
            'config': self.config.__dict__
        }
        node_ref.set(node_data)
        logger.info(f"Node registered: {self.config.node_id}")
    
    def _init_web3(self):
        """Initialize Web3 connection"""
        rpc_url = os.getenv('ARBITRUM_RPC_URL', 'https://sepolia-rollup.arbitrum.io/rpc')
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not self.web3.is_connected():
            logger.error("Web3 connection failed")
            raise ConnectionError("Failed to connect to Arbitrum")
            
        logger.info(f"Web3 connected to chain {self.web3.eth.chain_id}")
        
    def _init_exchanges(self):
        """Initialize cryptocurrency exchanges"""
        self.exchanges = {
            'binance': ccxt.binance(),
            'uniswap': None  # Will be handled via web3
        }
        
    async def monitor_opportunities(self):
        """Monitor for arbitrage opportunities"""
        logger.info("Starting opportunity monitoring")
        
        while True:
            try:
                self.metrics['opportunities_scanned'].inc()
                
                # Get prices from multiple sources
                cex_prices = await self._get_cex_prices()
                dex_prices = await self._get_dex_prices()
                
                # Find arbitrage opportunities
                opportunities = self._find_arbitrage(cex_prices, dex_prices)
                
                # Filter by risk parameters
                filtered_ops = self._filter_opportunities(opportunities)
                
                # Publish to Firebase
                if filtered_ops and self.rtdb:
                    await self._publish_opportunities(filtered_ops)
                
                # Execute if profitable
                for opp in filtered_ops[:3]:  # Limit to top 3
                    await self._execute_if_profitable(opp)
                
                await asyncio.sleep(1)  # Rate limit
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(5)
    
    async def _get_cex_prices(self) -> Dict[str, float]:
        """Get prices from centralized exchanges"""
        prices = {}
        try:
            ticker = self.exchanges['binance'].fetch_ticker('ETH/USDT')
            prices['ETH/USDT'] = ticker['last']
        except Exception as e:
            logger.warning(f"CEX price fetch failed: {e}")
        return prices
    
    async def _get_dex_prices(self) -> Dict[str, float]:
        """Get prices from decentralized exchanges (Uniswap V3)"""
        # Simplified: Using Chainlink price feeds for MVP
        prices = {}
        try:
            # Chainlink ETH/USD price feed on Arbitrum Sepolia
            price_feed_address = '0x5FbDB2315678afecb367f032d93F642f64180aa3'  # Mock
            # In production, use actual Chainlink ABI and address
            prices['ETH/USD'] = 1800.0  # Mock price
        except Exception as e:
            logger.warning(f"DEX price fetch failed: {e}")
        return prices
    
    def _find_arbitrage(self, cex_prices: Dict, dex_prices: Dict) -> list:
        """Identify arbitrage opportunities"""
        opportunities = []
        
        # Simple cross-exchange arbitrage detection
        if 'ETH/USDT' in cex_prices and 'ETH/USD' in dex_prices:
            cex_price = cex_prices['ETH/USDT']
            dex_price = dex_prices['ETH/USD']
            
            # Account for 1% spread threshold
            spread = abs(cex_price - dex_price) / min(cex_price, dex_price)
            
            if spread > 0.01:  # 1% spread
                opportunities.append({
                    'pair': 'ETH/USDT',
                    'cex_price': cex_price,
                    'dex_price': dex_price,
                    'spread': spread,
                    'direction': 'buy_cex_sell_dex' if cex_price < dex_price else 'buy_dex_sell_cex',
                    'timestamp': datetime.utcnow().isoformat()
                })
                
        return opportunities
    
    def _filter_opportunities(self, opportunities: list) -> list:
        """Filter opportunities through risk engine"""
        filtered = []
        
        for opp in opportunities:
            # Skip if spread too small
            if opp['spread'] < 0.01:
                continue
                
            # Estimate gas cost (mock)
            gas_cost_usd = 0.1  # Conservative estimate
            
            # Estimate profit (0.1% of $10 capital)
            position_size = min(self.config.max_capital_usd * 0.001, 0.01)
            estimated_profit = position_size * opp['spread']
            
            # Check against failure modes
            gas_check = self.failure_handler.handle_gas_spike(30)  # Mock gas
            profit_check = self.failure_handler.handle_micro_profit(estimated_profit, gas_cost_usd)
            
            if gas_check['action'] == 'proceed' and profit_check['action'] == 'proceed':
                opp['estimated_profit'] = estimated_profit
                opp['position_size'] = position_size
                opp['gas_cost'] = gas_cost_usd
                filtered.append(opp)
                
        return filtered
    
    async def _publish_opportunities(self, opportunities: list):
        """Publish opportunities to Firebase Realtime DB"""
        try:
            for opp in opportunities:
                path = f"/signals/421614/ETH_USDT/{datetime.utcnow().timestamp()}"
                self.rtdb.child(path).set(opp)
        except Exception as e:
            logger.error(f"Failed to publish opportunities: {e}")
    
    async def _execute_if_profitable(self, opportunity: Dict):
        """Execute trade if it passes all checks"""
        try:
            # Final pre-execution checks
            current_gas = self.web3.eth.gas_price
            gas_check = self.failure_handler.handle_gas_spike(
                self.web3.from_wei(current_gas, 'gwei')
            )
            
            if gas_check['action'] != 'proceed':
                logger.info(f"Skipping due to gas: {gas_check}")
                return
            
            # Mock execution for MVP
            logger.info(f"Executing opportunity: {opportunity}")
            
            # Record execution
            if self.firestore:
                execution_data = {
                    'node_id': self.config.node_id,
                    'opportunity': opportunity,
                    'executed_at': datetime.utcnow().isoformat(),
                    'profit_estimated': opportunity['estimated_profit'],
                    'status': 'executed'
                }
                self.firestore.collection('executions').add(execution_data)
            
            self.metrics['trades_executed'].inc()
            self.metrics['profits_usd'].inc(opportunity['estimated_profit'])
            
            # Auto-sweep profits to hardware fund
            await self._sweep_profits()
            
        except Exception as e:
            logger.error(f"Execution failed: {e}")
    
    async def _sweep_profits(self):
        """Auto-sweep profits to hardware fund"""
        # Mock implementation - in production would interact with vault contract
        logger.info("Auto-sweeping profits to hardware fund")
        
        if self.firestore:
            sweep_data = {
                'node_id': self.config.node_id,
                'amount': 0.001,  # Mock amount
                'timestamp': datetime.utcnow().isoformat(),
                'destination': 'hardware_fund'
            }
            self.firestore.collection('sweeps').add(sweep_data)
    
    async def send_heartbeat(self):
        """Send periodic heartbeat to Firebase"""
        while True:
            try:
                if self.firestore:
                    self.firestore.collection('nodes').document(self.config.node_id).update({
                        'last_heartbeat': datetime.utcnow().isoformat()
                    })
                
                # Update capital metric
                self.metrics['node_capital'].set(self.config.max_capital_usd)
                
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
            
            await asyncio.sleep(self.config.heartbeat_interval)
    
    async def run(self):
        """Main run loop"""
        logger.info(f"Starting Mycelium Node {self.config.node_id}")
        
        # Start metrics server
        start_http_server(8000)
        
        # Run tasks concurrently
        await asyncio.gather(
            self.monitor_opportunities(),
            self.send_heartbeat()
        )

async def main():
    """Entry point"""
    # Generate unique node ID
    node_id = f"mycelium_{os.uname().nodename}_{int(datetime.utcnow().timestamp())}"
    
    # Configuration
    config = NodeConfig(
        node_id=node_id,
        vault_address=os.getenv('VAULT_ADDRESS', '0x0000000000000000000000000000000000000000'),
        max_capital_usd=float(os.getenv('MAX_CAPITAL_USD', '10.0')),
        min_profit_multiplier=float(os.getenv('MIN_PROFIT_MULTIPLIER', '2.0')),
        max_gas_gwei=int(os.getenv('MAX_GAS_GWEI', '50'))
    )
    
    # Create and run node
    node = MyceliumNode(config)
    await node.run()

if __name__ == "__main__":
    asyncio.run(main())