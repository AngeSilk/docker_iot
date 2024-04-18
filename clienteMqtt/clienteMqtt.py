import asyncio, ssl, certifi, logging, os
from asyncio_mqtt import Client, ProtocolVersion

logging.basicConfig(format='%(asctime)s - cliente mqtt - %(levelname)s:%(message)s', level=logging.INFO, datefmt='%d/%m/%Y %H:%M:%S %z')

async def main():
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.minimum_version = ssl.TLSVersion.TLSv1_2
    tls_context.maximum_version = ssl.TLSVersion.TLSv1_3
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()

    async with Client(
        os.environ['SERVIDOR'],
        protocol=ProtocolVersion.V31,
        port=8883,
        tls_context=tls_context
    ) as client:
        async with client.messages() as messages:
            await client.subscribe(os.environ['TOPICO'])
            async for message in messages:
                logging.info(str(message.topic) + ": " + message.payload.decode("utf-8"))

if __name__ == "__main__":
    asyncio.run(main())
