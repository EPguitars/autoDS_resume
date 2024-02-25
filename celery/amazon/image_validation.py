import logging
import ssl

import httpx

from headers import headers
from tools import proxy_gen

IMAGE_EXTENSIONS = [
    'image/jpeg',
    'image/png',
    'image/gif',
    'image/bmp',
    'image/webp',
    'image/svg+xml',
    'image/tiff',
    'image/x-icon',
    'image/vnd.microsoft.icon',
    'image/jxr',
    'image/jp2',
    'image/heif',
    'image/heic',
    'image/avif']


async def validate_image(url, attempts=3):
    """ parse image url """
    if url:
        async with httpx.AsyncClient(headers=headers, 
                                         follow_redirects=True) as img_client:

            try:
                    
                logging.info("sending img validation request")
                response = await img_client.head(url, timeout=60)
                logging.info("recieved img response")

                
            except ValueError:
                logging.warning("Img value error")
                return None

            except (httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.ConnectError,
                    httpx.ReadError):
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Connection error, retrying request in image validation")
                    return await validate_image(url, attempts)

                else:
                    logging.warning("Problems with connection when image checking")
                    return None
        
            except httpx.ProxyError:
                logging.error("Proxy error")
                if attempts > 0:
                    attempts -= 1
                    logging.warning("Proxy error, retrying request in image validation")
                    return await validate_image(url, attempts)

            except httpx.UnsupportedProtocol:
                logging.warning("Protocol error")
                return None

            except ssl.SSLError:
                logging.warning("SSL Error")
                if attempts > 0:
                    attempts -= 1
                    logging.warning("SSL error, retrying request in image validation")
                    return await validate_image(url, attempts)
            
            except httpx.RemoteProtocolError:
                logging.warning("Remote protocol error")
                return None

            except Exception as exc:
                logging.warning(exc.with_traceback())
                logging.warning("Exception in  image validator")
        
        if response.status_code == 200 \
                and response.headers["content-type"] in IMAGE_EXTENSIONS:

            return url
        
        else:
            logging.warning("Bad img url!")
            return False

    else:
        return None