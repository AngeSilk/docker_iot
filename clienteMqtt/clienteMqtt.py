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

async def distributor(client, topico, subtopicos):

    async for message in client.messages:
        if message.topic.matches(topico + "/"+ subtopicos[0]):
            logging.info(message.payload)
            contador_queue.put_nowait(message)
        elif message.topic.matches(topico + "/"+ subtopicos[1]):
            logging.info(message.payload)
            timestamp_queue.put_nowait(message)

contador_queue = asyncio.Queue()
timestamp_queue = asyncio.Queue()

async def main():
    
    topico = os.environ['TOPICO']
    subtopicos = str(os.getenv("TOPICOS")).split(",")
    server = os.environ['SERVIDOR']
    
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls_context.verify_mode = ssl.CERT_REQUIRED
    tls_context.check_hostname = True
    tls_context.load_default_certs()

    async with aiomqtt.Client(
        server,
        port=8883,
        tls_context=tls_context
    ) as client:
        try:
            logging.info(str(subtopicos))
            for topic in subtopicos:
                await client.subscribe(topico + "/"+ topic)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(distributor(client, topico, subtopicos))
                tg.create_task(contador())
                tg.create_task(timestamp())

        except KeyboardInterrupt:
            logging.info("Detetindo por el ususario")
        except Exception as e:
            logging.error(f"Se ha producido un error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())