import asyncio, ssl, certifi, logging, os
import aiomqtt, time

logging.basicConfig(format='%(asctime)s - cliente mqtt - %(levelname)s:%(message)s', level=logging.INFO, datefmt='%d/%m/%Y %H:%M:%S %z')

async def contador():
    count = 0
    while True:
        print(f"Estado del contador: {count}")
        count += 1
        await asyncio.sleep(1)  # Espera 1 segundos antes de continuar
        return count

async def timestamp():
   return time.time()

async def mqtt():
    
    async with aiomqtt.Client(
        server,
        port=8883,
        tls_context=tls_context
    ) as client:
        logging.info(subtopicos)
        for topic in subtopicos:
            await client.subscribe(topico + "/"+ topic)
        async for message in client.messages:
            logging.info(str(message.topic) + ": " + message.payload.decode("utf-8"))

async def main():
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()
    logging.info(subtopicos)
    
    topico = os.environ['TOPICO']
    subtopicos = str(os.getenv("TOPICOS")).split(",")
    server = os.environ['SERVIDOR']

    async with asyncio.TaskGroup() as tg:
        task1 = tg.create_task(contador())
        task2 = tg.create_task(timestamp())
        task3 = tg.create_task(mqtt)

    logging.info(task1.result())
    
if __name__ == "__main__":
    asyncio.run(main())