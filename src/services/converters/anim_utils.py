import os
import asyncio
import logging
from PIL import Image
from typing import Optional

logger = logging.getLogger(__name__)


def has_webp_anim_container(filepath: str) -> bool:
    """
    Check if a WebP file has an animation container (VP8X ANIM flag set).
    """
    try:
        with open(filepath, "rb") as f:
            header = f.read(30)
            if len(header) >= 21 and header[:4] == b"RIFF" and header[8:12] == b"WEBP":
                if header[12:16] == b"VP8X":
                    flags = header[20]
                    return bool(flags & 0x02)  # Bit 1 is Animation flag
        return False
    except Exception:
        return False

def is_animated_webp(filepath: str, sanitize_single_frame: bool = True) -> bool:
    """
    Check if a WebP file is animated (>= 2 frames).
    If it is a single frame WebP with an animation container and sanitize_single_frame is True,
    it converts it in-place to a genuine static WebP and returns False.
    """
    temp_path = f"{filepath}.tmp.webp"
    try:
        with Image.open(filepath) as im:
            n_frames = getattr(im, "n_frames", 1)
            
            if n_frames > 1:
                return True
            
        # Single-frame WebP: check if it's trapped in an animation container
        if sanitize_single_frame and has_webp_anim_container(filepath):
            logger.info("Sanitizing single frame animated WebP to pure static WebP: %s", filepath)
            with Image.open(filepath) as im:
                im.save(temp_path, "WEBP")
            os.replace(temp_path, filepath)
            
        return False

    except Exception as e:
        logger.warning("Could not check animation status of %s: %s", filepath, e)
        return False
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass



async def wrap_static_as_animated(filepath: str, output_path: Optional[str] = None) -> bool:
    """
    Wrap a static WebP as a 2-frame looping animated WebP using webpmux.
    WhatsApp requires atleast 2 frames to accept an animated sticker.    
    """
    if output_path is None:
        output_path = filepath
    
    temp_static_path = f"{filepath}.static.webp"

    try:
        # make it a staic webp (in case of 1 frame animated webps)
        with Image.open(filepath) as img:
            img.save(temp_static_path, 'WEBP')

        process = await asyncio.create_subprocess_exec(
            'webpmux',
            '-frame', temp_static_path, '+100+0+0+0+b',
            '-frame', temp_static_path, '+100+0+0+0+b',
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
    finally:
        if os.path.exists(temp_static_path):
            try:
                os.remove(temp_static_path)
            except Exception:
                pass