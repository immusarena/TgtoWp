import asyncio
import struct
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def is_animated_webp(filepath: str) -> bool:
    """
    Check if a WebP file is animated by reading just the file header.
    Looks for the VP8X extended chunk and checks the animation flag bit.
    Only reads 24 bytes from disk, no decoding.
    """
    try:
        with open(filepath, 'rb') as f:
            header = f.read(24)
            if len(header) < 21:
                return False
            # Verify RIFF/WEBP container
            if header[:4] != b'RIFF' or header[8:12] != b'WEBP':
                return False
            # Check if first chunk is VP8X (extended format)
            if header[12:16] != b'VP8X':
                return False
            # VP8X flags are 4 bytes at offset 20 (little-endian)
            # Animation flag is bit 1 (0x02)
            flags = struct.unpack('<I', header[20:24])[0]
            return bool(flags & 0x02)
    except Exception as e:
        logger.warning("Could not check animation status of %s: %s", filepath, e)
        return False


async def wrap_static_as_animated(filepath: str, output_path: Optional[str] = None) -> bool:
    """
    Wrap a static WebP as a 2-frame looping animated WebP using webpmux.
    WhatsApp requires atleast 2 frames to accept an animated sticker.    
    """
    if output_path is None:
        output_path = filepath
    
    try:
        process = await asyncio.create_subprocess_exec(
            'webpmux',
            '-frame', filepath, '+100+0+0+0+b',
            '-frame', filepath, '+100+0+0+0+b',
            '-loop', '0',
            '-o', output_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
            
            if process.returncode == 0:
                return True
            else:
                logger.error("webpmux failed (rc=%d) for %s: %s", process.returncode, filepath, stderr.decode().strip())
                return False
                
        except asyncio.TimeoutError:
            logger.error("webpmux timed out after 10s: %s", filepath)
            try:
                process.kill()
                await process.wait()
            except Exception:
                pass
            return False
            
    except Exception as e:
        logger.error("Failed to wrap static WebP as animated: %s Error: %s", filepath, e)
        return False
