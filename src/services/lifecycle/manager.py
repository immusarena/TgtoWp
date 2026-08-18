import sys
import shutil
import asyncio
import logging
from src.db import close_pool
from src.bot.handlers.context import BotContext

logger = logging.getLogger(__name__)

PM2_APP_NAME = "tgbot"

class LifecycleManager:
    def __init__(self, ctx: BotContext):
        self.state = "normal"
        self.ctx = ctx

    def get_state(self):
        return self.state

    async def _invoke_pm2_stop(self, app_name: str = PM2_APP_NAME):
        """
        Instructs the PM2 to set the app state to 'stopped' 
        so PM2 does not auto restart it upon exit.
        """
        if not shutil.which("pm2"):
            logger.debug("PM2 is not installed or not in PATH. Skipping pm2 stop.")
            return
        try:
            logger.info(f"Sending 'pm2 stop {app_name}' command...")
            proc = await asyncio.create_subprocess_exec(
                "pm2", "stop", app_name,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            # Give it up to 2 seconds to deliver the stop message to PM2
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning("Could not execute pm2 stop: timeout exceeded.")
                pass
        except Exception as e:
            logger.warning(f"Could not execute pm2 stop: {e}")

    async def _invoke_pm2_restart(self, app_name: str = PM2_APP_NAME):
        """
        Instructs the PM2 to restart the app instanly after it exits, 
        rather than waiting for PM2 to detect it as a crash
        and then restart it which may be delayed if restart_delay is set,
        or may not restart at all if autorestart is turned off.
        """
        if not shutil.which("pm2"):
            logger.debug("PM2 is not installed or not in PATH. Skipping pm2 restart.")
            return
        try:
            logger.info(f"Sending 'pm2 restart {app_name}' command...")
            proc = await asyncio.create_subprocess_exec(
                "pm2", "restart", app_name,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            # Give it up to 2 seconds to deliver the restart message to PM2
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning("Could not execute pm2 restart: timeout exceeded.")
                pass
        except Exception as e:
            logger.warning(f"Could not execute pm2 restart: {e}")

    async def handle_shutdown(self, immediate: bool = False, call_pm2_stop: bool = True):
        """
        Handles the shutdown logic.
        """    
        
        if not immediate:
            self.state = "graceful_shutdown"
            logger.warning("Graceful shutdown initiated. Finishing current task...")
            # Signal the queue processor to stop picking up new items
            self.ctx.shutting_down = True
            
            # Wait for the current item to finish processing
            async with self.ctx.processing_lock:
                logger.warning("Current task finished. Proceeding with shutdown.")
                # Now we can safely shut down everything else
                # release lock and notify channel
                if self.ctx.ha_manager:
                    await self.ctx.ha_manager.release_and_notify()
                # release the database pool
                await close_pool()
                # To prevent SIGINT from pm2 stop from being catched by the signal handler
                self.state = 'almost_done_shutdown'
                # mark process as stopped so PM2 doesnt auto restart it
                if call_pm2_stop:
                    await self._invoke_pm2_stop()
                # disconnect from telegram (remember as soon as this is called the run_until_disconnected() in main will be unblocked, so we use it at last)
                if self.ctx.client.is_connected():
                    await self.ctx.client.disconnect()
                
                logger.info("All resources released cleanly.")

        else:
            self.state = "immediate_shutdown"
            logger.critical("IMMEDIATE SHUTDOWN INITIATED")
            # Don't wait for queue, just release the resources and exit immediately
            # release lock and notify channel
            if self.ctx.ha_manager:
                await self.ctx.ha_manager.release_and_notify()
            # release the database pool
            await close_pool()
            # mark process as stopped so PM2 doesnt auto restart it
            if call_pm2_stop:
                await self._invoke_pm2_stop()
            # disconnect from telegram (remember as soon as this is called the run_until_disconnected() in main will be unblocked, so we use it at last)
            if self.ctx.client.is_connected():
                await self.ctx.client.disconnect()
            
            logger.info("All resources released cleanly.")

    async def handle_restart(self, immediate=False, call_pm2_restart=True):
          
        if not immediate:
            self.state = "graceful_restart"
            logger.warning("Graceful restart initiated. Finishing current task...")
            # Signal the queue processor to stop picking up new items
            self.ctx.shutting_down = True
            
            # Wait for the current item to finish processing
            async with self.ctx.processing_lock:
                logger.warning("Current task finished. Proceeding with restart.")
                # Now we can safely shut down everything else
                # release lock and notify channel
                if self.ctx.ha_manager:
                    await self.ctx.ha_manager.release_and_notify()
                # release the database pool
                await close_pool()
                # To prevent SIGINT from pm2 restart from being catched by the signal handler
                self.state = 'almost_done_restart'
                # call pm2 restart for immidiate restart rathter than waiting for pm2 to detect it as a crash and then restart it
                if call_pm2_restart:
                    await self._invoke_pm2_restart()
                # disconnect from telegram (remember as soon as this is called the run_until_disconnected() in main will be unblocked, so we use it at last)
                if self.ctx.client.is_connected():
                    await self.ctx.client.disconnect()
                
                logger.info("All resources released cleanly.")

        else:
            self.state = "immediate_restart"
            logger.critical("IMMEDIATE RESTART INITIATED")
            # Don't wait for queue, just release the resources and exit immediately
            # release lock and notify channel
            if self.ctx.ha_manager:
                await self.ctx.ha_manager.release_and_notify()
            # release the database pool
            await close_pool()
            # call pm2 restart for immidiate restart rathter than waiting for pm2 to detect it as a crash and then restart it
            if call_pm2_restart:
                await self._invoke_pm2_restart()
            # disconnect from telegram (remember as soon as this is called the run_until_disconnected() in main will be unblocked, so we use it at last)
            if self.ctx.client.is_connected():
                await self.ctx.client.disconnect()
            
            logger.info("All resources released cleanly.")
        
        