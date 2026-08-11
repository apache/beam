# Propuesta Técnica: Optimización de Cómputo y Eficiencia de Runners para Pruebas de Streaming (Taxi Rides)

## 2. El Nuevo Administrador de Limpieza de Pruebas en Python (TestPubsubContext)

## 2.1 Diferencia Clave con stale_cleaner.py

Es fundamental distinguir el propósito de este nuevo módulo frente al script de mantenimiento que ya implementamos:

| Característica | `stale_cleaner.py` (Mantenimiento Reactivo) | `TestPubsubContext.py` (Manejador Proactivo) |
| :--- | :--- | :--- |
| Enfoque | Reactivo / Housekeeping: Limpia de manera periódica (ej. cada noche) recursos acumulados por fallos del pasado. | Proactivo / Lifecycle: Previene la existencia de fugas controlando el ciclo de vida del recurso *durante* el test. |
| Uso | Tarea cron programada externa que lee de listas fijas de prefijos y cubetas de Storage. | Clase y decorador importable que los desarrolladores usan al escribir cualquier nuevo test de Python. |
| Código | Estático y global para todo el proyecto. | Reutilizable y modular para erradicar la duplicación de código redundante de limpieza en los scripts de prueba. |


## 2.2 Implementación de la "Triple Capa de Seguridad"

Para evitar que las pruebas de integración en Python dejen recursos huérfanos cuando la infraestructura de red o el runner de CI fallan abruptamente, el nuevo manejador de Python implementa un enfoque de seguridad redundante de tres capas:

- Capa 1: Prevención Activa (Context Manager): Uso del protocolo estándar with de Python. Ante cualquier falla de código (asserts o timeouts), la función __exit__ se ejecuta de manera garantizada y destruye la suscripción y el tema de forma inmediata.
- Capa 2: Mitigación de Caídas Catastróficas (Barrido Preventivo): Si la máquina de CI se apaga de golpe por completo, se genera una fuga. Para resolver esto, cuando una prueba "vuelve a habilitarse" e inicia, el método _limpiar_recursos_huerfanos_previos() del contexto barre y elimina proactivamente cualquier recurso huérfano del pasado con prefijos de prueba antes de crear la infraestructura nueva.
- Capa 3: Autodestrucción Pasiva (GCP TTL): Al crearse el recurso en GCP, el script le asocia un TTL corto de inactividad de 1 día (expiration_policy). Si las Capas 1 y 2 fallaran, la nube de Google destruye físicamente el recurso por inactividad tras 24 horas.

## 3.3 Propuesta del código

```bash
import time
from google.cloud import pubsub_v1

class TestPubsubContext:
    """Equivalent to Java's TestPubsub for Python Integration Tests.
    Implements a Triple-Layer Security model to eliminate GCP resource leaks.
    """
    def __init__(self, project_id, prefix="integ-test-python-"):
        self.project_id = project_id
        self.project_path = f"projects/{project_id}"
        self.prefix = prefix
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.topic_path = None
        self.subscription_path = None

    def _limpiar_recursos_huerfanos_previos(self):
        """CAPA 2: Barrido preventivo. 
        Al volver a habilitarse la prueba, limpia de manera automática 
        recursos huérfanos de ejecuciones fallidas pasadas.
        """
        print(f"[Seguridad] Iniciando barrido de recursos huérfanos con prefijo '{self.prefix}'...")
        try:
            for sub in self.subscriber.list_subscriptions(request={"project": self.project_path}):
                sub_name = sub.name.split("/")[-1]
                if sub_name.startswith(self.prefix):
                    print(f"[Seguridad - Limpieza] Eliminando suscripción huérfana inactiva: {sub.name}")
                    try:
                        self.subscriber.delete_subscription(request={"subscription": sub.name})
                    except Exception:
                        pass
        except Exception as e:
            print(f"[Seguridad] Error durante el barrido inicial: {e}")

    def __enter__(self):
        # Ejecutar la limpieza preventiva de fallas del pasado antes de iniciar la prueba actual
        self._limpiar_recursos_huerfanos_previos()

        # Crear Tema único aleatorio para la prueba actual
        unique_id = int(time.time() * 1000)
        topic_id = f"{self.prefix}topic-{unique_id}"
        self.topic_path = self.publisher.topic_path(self.project_id, topic_id)
        self.publisher.create_topic(request={"name": self.topic_path})
        print(f"[Contexto] Tema de prueba creado de forma segura: {self.topic_path}")
        
        # Crear Suscripción única aleatoria
        sub_id = f"{self.prefix}sub-{unique_id}"
        self.subscription_path = self.subscriber.subscription_path(self.project_id, sub_id)
        
        # CAPA 3: TTL pasivo de 1 día de expiración automática de GCP en caso de fallo absoluto de CI/CD
        expiration_policy = pubsub_v1.types.ExpirationPolicy(
            ttl=pubsub_v1.types.Duration(seconds=86400)
        )
        self.subscriber.create_subscription(
            request={
                "name": self.subscription_path,
                "topic": self.topic_path,
                "expiration_policy": expiration_policy
            }
        )
        print(f"[Contexto] Suscripción de prueba creada de forma segura: {self.subscription_path}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # CAPA 1: Teardown Activo garantizado por el Context Manager de Python
        print("\n[Teardown] Iniciando limpieza de recursos de prueba actual...")
        try:
            if self.subscription_path:
                self.subscriber.delete_subscription(request={"subscription": self.subscription_path})
                print(f"[Teardown] Suscripción eliminada: {self.subscription_path}")
            if self.topic_path:
                self.publisher.delete_topic(request={"topic": self.topic_path})
                print(f"[Teardown] Tema eliminado: {self.topic_path}")
        except Exception as e:
            print(f"[Teardown Error] No se pudieron eliminar los recursos de la prueba actual: {e}")
```