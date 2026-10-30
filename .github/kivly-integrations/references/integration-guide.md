# Kivly Public API — integración de servidor

Este kit corresponde al contrato 1.1.0 incluido. Despliega primero el backend que lo implementa antes de actualizar ReadMe o usar los nuevos endpoints. La documentación histórica no demuestra qué versión está desplegada.

## Credenciales y primera petición

Obtén `client_id` y `client_secret` en Dashboard → Integraciones → Desarrolladores, dentro del negocio correcto. Crea una clave con nombre, permisos por familia de recursos y caducidad. Cada clave pertenece a un negocio (`location_id`), no a una sucursal. El secreto se muestra una sola vez. Las credenciales antiguas del negocio siguen siendo compatibles. Guarda el secreto en un gestor de secretos del servidor. No lo incluyas en aplicaciones móviles, navegador, repositorios, capturas ni registros.

Una cuenta de prueba creada en el panel normal utiliza producción: `https://api.getkivly.com`. El kit no inventa un host de staging ni presupone que sus credenciales funcionen allí.

```http
POST /api/v1/auth/token
Content-Type: application/json

{"client_id":"YOUR_CLIENT_ID","client_secret":"YOUR_CLIENT_SECRET"}
```

Respuesta de ejemplo, con identificadores ficticios:

```json
{
  "id": "64a000000000000000000002",
  "message": "Authentication successful with client id and secret token",
  "status": 200,
  "success": true,
  "data": {
    "token": "<JWT>",
    "token_type": "Bearer",
    "expires_in": 3600,
    "expires_at": "2026-10-24T12:00:00Z",
    "owner": "64a000000000000000000002",
    "location_id": "64a000000000000000000001"
  }
}
```

Usa `Authorization: Bearer <data.token>`. No hay `grant_type`, formulario OAuth, `access_token` en la raíz ni refresh token. Cachea el token y renueva unos 30 segundos antes de su vencimiento; no solicites uno por operación. Rotar `client_id` o `client_secret` invalida los nuevos tokens de integración ya emitidos. El secreto de firma de webhooks es independiente.

Comprueba `GET /api/v1/integration/context` y después `GET /api/v1/branches`. El contexto devuelve el negocio, los límites configurados y las familias de recursos. Los tokens no sirven para GraphQL, sesiones del Dashboard ni endpoints administrativos. Un `location_id` diferente produce 403; las referencias a recursos ajenos se rechazan. Las claves nuevas comprueban permisos `familia:read` o `familia:write` en cada petición; escritura incluye lectura. La revocación, caducidad o rotación invalidan sus tokens. Las credenciales antiguas conservan acceso a las operaciones publicadas del negocio. Todas comparten los límites del negocio.

## Recursos

Busca cada operación y su esquema exacto en `openapi.json`; no hay una envoltura universal de respuesta ni una paginación universal en los endpoints históricos.

| Familia | Recursos |
| --- | --- |
| Clientes y CRM | Clientes, listas de contactos y conversión a clientes |
| Fidelización | Puntos, saldos, recompensas, canjes, registro de ventas y promociones |
| Catálogo | Menús, secciones, productos, modificadores, grupos y combos |
| Facturación | [Borradores, emisión, sustitución, rectificación, anulación, estado fiscal y PDF](billing.md) |
| Marketing | [Campañas, notificaciones automáticas, tarjetas regalo y encuestas](marketing.md) |
| Operación | [Pedidos de webs externas al TPV](orders.md), estados, devoluciones, sucursales y consulta de turnos |
| Reservas | Disponibilidad, turnos, reglas, cancelaciones, zonas, planos, mesas, combinaciones y lista de espera |
| Analítica | Ventas, fidelización, catálogo y exportaciones publicadas |
| Costes | Gastos, resultados, análisis, objetivos, inversiones, proveedores, ingredientes, recetas, platos, stock, documentos y mermas |
| Integración | Contexto, configuración de webhooks, eventos, historial y reintento manual |

Los endpoints `/public/*` son flujos públicos existentes y pueden tener validaciones específicas o tokens de cliente; consulta `security` y la descripción de cada operación. No intercambies tokens de cliente, de integración y de sesión. Para importar pedidos de tu web al TPV usa `POST /api/v1/orders` con `orders:write`; consulta [el flujo de pedidos de canal](orders.md). `/public/orders/*` es el checkout de Kivly y no se publica para integradores.

## Límites y errores

Valores predeterminados del backend; `integration/context` informa de los límites efectivos:

- Token: 20 solicitudes/minuto por dirección de origen vista por el servidor.
- API de integración: 120 solicitudes/minuto y 5.000/hora por negocio, compartidas entre sus tokens.
- Pruebas de webhook: 5/minuto por negocio.
- Reintentos manuales de webhook: 5/minuto por negocio.

Las ventanas empiezan con la primera petición contabilizada. 429 incluye `Retry-After` en segundos. Espera ese plazo y añade jitter; limita reintentos y concurrencia. Los límites del proxy/WAF pueden ser adicionales. Si el almacén de límites no está disponible, las integraciones reciben 503 y `Retry-After: 5`.

| HTTP | Acción |
| --- | --- |
| 401 | Revisar credenciales, vencimiento o rotación; obtener un token nuevo una vez |
| 403 | Revisar cuenta/negocio activo, acceso de facturación, ubicación y endpoint permitido |
| 404 | Verificar ID y negocio; no buscar recursos de otras cuentas |
| 409 | Volver a consultar el estado antes de decidir una nueva acción |
| 422 | Corregir campos, referencias, formato y reglas de negocio |
| 429 | Respetar Retry-After; reducir ritmo |
| 503 | Esperar de forma acotada y consultar el estado de la operación |

`{"detail":"Access denied"}` no identifica por sí solo la causa. El endpoint de token de esta versión no exige registrar una IP ni una activación separada de integración. No elimina controles de cuenta o del perímetro. Si persiste un 403, recoge hora UTC, método, ruta, estado, `X-Kivly-Request-Id`, `X-Request-ID` y `CF-Ray`; comunica el `client_id`/ID de negocio por soporte, nunca el secreto ni el Authorization. Una cuenta de prueba debe cumplir los controles de acceso vigentes.

## Reintentos e idempotencia

No existe un `Idempotency-Key` genérico. Reintenta lecturas 429/503 con espera acotada. Ante timeout en una escritura, consulta si se aplicó antes de repetir: crear gastos, inversiones o movimientos puede duplicar operaciones. `POST /api/v1/orders` tiene deduplicación propia por negocio, `channel` y `external_id`: repite el cuerpo original y conserva esa identidad; un cuerpo distinto devuelve 409. Consulta [pedidos](orders.md) para recuperarte de un timeout o fallo de sincronización del TPV. El registro de mermas tiene su propio `request_id`; conserva el mismo valor y cuerpo al reintentarlo. El código de venta de fidelización debe ser estable y único, pero no sustituye una garantía transaccional general ante carreras concurrentes.

Las paginaciones nuevas usan límites de hasta 100. Catálogo `/catalog/*`: `skip`, `limit`, `has_next`. Catálogos de costes: `page` desde 1, `limit`, `total`, `has_next`. Gastos: `offset`, `limit`, `has_more`, `next_offset`. Documentos: `page` desde 1, `limit`, `total`, `has_more`. Mermas: `page` desde 0 y `has_next`. Historial de webhooks: `page` desde 1, `limit`, `has_next`.

## Pruebas del integrador

1. Obtener token y comprobar negocio/límites en contexto.
2. Leer una página de recursos propios y comprobar que una referencia ajena se rechaza.
3. Ejecutar una escritura autorizada en una cuenta de pruebas y leer el resultado.
4. Verificar firma de webhook sobre bytes originales, duplicados y reintento manual de un fallo.
5. Simular 401, 429, timeout y 503 en el cliente, sin generar carga real para agotar los límites.
6. Revisar unidad monetaria, paginación, zona horaria y respuestas parciales del recurso utilizado.

El script `kivly_client.py --smoke` sólo hace lecturas de contexto y sucursales; no sustituye estas pruebas funcionales. No se incluyen credenciales ni se afirma una certificación de producción.

## Consola de desarrolladores

Dashboard → Integraciones → Desarrolladores permite crear claves, consultar actividad y configurar varios destinos de webhook por negocio. Cada destino tiene URL, suscripciones y secreto de firma independiente. Su secreto se muestra al crear o rotar; guárdalo en tu servidor. Las operaciones `/developer/*` requieren sesión de propietario del Dashboard y no están disponibles con un token de integración.

Los registros técnicos de API y eventos se conservan 30 días. Incluyen identificador de petición, ruta parametrizada, método, estado y latencia; los cuerpos se limitan y ocultan secretos y datos personales. No son una copia completa de la petición. Las solicitudes de token rechazadas antes de autenticar no se atribuyen a un negocio.

Puedes usar `POST /api/v1/webhooks/kivly` como destino de prueba: admite hasta 256 KiB, descarta el cuerpo y devuelve HTTP 200 con texto `ok`, sin autenticación ni efectos de negocio. Su límite es 300 peticiones/minuto por dirección de origen. No valida firmas; comprueba aparte la verificación en tu receptor real.
