### Objetivo

Demostrar la viabilidad y las ventajas económicas y de diseño de reemplazar la lógica de limpieza manual y fragmentada de `pubsub_io_perf_test.py` por el nuevo manejador centralizado `TestPubsubContext`.

### Tabla de Comparativa Técnica:

| Escenario de Ejecución | Comportamiento Actual (Sin Manejador) | Comportamiento Optimizado (Con `TestPubsubContext`) |
| :--- | :--- | :--- |
| El test pasa con éxito | Se ejecuta el método `cleanup()`. Los recursos se borran (Escenario saludable). | Se ejecuta el método `__exit__` del contexto. Los recursos se borran de inmediato de GCP. |
| El test falla por timeout / error | El script se interrumpe bruscamente. El método `cleanup()` NO se ejecuta, provocando una fuga de recursos activa. | El Context Manager intercepta la excepción, retiene los recursos por 24 horas para debugging y luego los auto-elimina. |
| El pipeline se cancela en la CI (Jenkins/GitHub) | La máquina virtual se destruye de golpe. No hay limpieza de recursos, generando almacenamiento fantasma por 31 días. | El TTL de 24 horas (`expiration_policy`) inyectado durante la creación por el contexto destruye de forma segura la suscripción en GCP. |
| Duplicación de Código | Cada clase (`Read` y `Write`) maneja de forma independiente y aislada la creación de sus clientes de GCP. | Se centraliza el monitoreo y registro de los recursos creados en un solo objeto `TestPubsubContext`. |

###