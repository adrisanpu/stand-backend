# Webhooks: configuración, recepción y recuperación

Consulta `GET /api/v1/webhooks/events`: es la fuente de eventos disponibles, agrupados por dominio. `GET /webhooks/config` devuelve URL, suscripciones y `has_secret`, nunca el secreto. `PUT /webhooks/config` recibe `url`, `events` y opcionalmente `secret` (16–1024 caracteres). Omitir `secret` conserva el existente. Una lista de eventos vacía desactiva los envíos.

Usa una URL HTTPS pública en puerto 443, sin credenciales ni fragmento. No se admiten IP privadas, loopback ni DNS que resuelva a redes privadas. Se comprueba y fija la IP para cada intento y no se siguen redirecciones. Configura directamente la URL final. Los endpoints de webhook del integrador no necesitan allowlist de IP de Kivly para la autenticación: comprueba la firma.

`POST /webhooks/test` con `{}` prueba la URL y el secreto guardados; acepta sustituciones explícitas `url` y `secret`. Envía `test.webhook` con datos ficticios. Su respuesta HTTP puede ser 200 con `success:false`: comprueba el booleano. La prueba no se registra en el historial de eventos de negocio ni se reintenta automáticamente.

## Firma y cuerpo

Cabeceras:

- `X-Kivly-Delivery-Id`: ID estable del evento, coincide con `body.id`.
- `X-Kivly-Event`: coincide con `body.event`.
- `X-Kivly-Timestamp`: instante Unix en segundos de este intento.
- `X-Kivly-Signature-V2`: HMAC-SHA256 hexadecimal de `timestamp + "." + raw_body`, usando el secreto de webhook.
- `X-Kivly-Signature`: compatibilidad anterior, HMAC-SHA256 sólo de `raw_body`.

Usa V2. Verifica sobre los bytes originales UTF-8 antes de parsear JSON y compara en tiempo constante. El ejemplo `scripts/verify_webhook.py` acepta un desfase máximo de 300 segundos y comprueba que los IDs y el tipo del evento coinciden. Sincroniza el reloj del receptor. La firma v1 no tiene protección temporal por sí sola.

El cuerpo incluye `id`, `event`, `timestamp` (creación del evento), `owner_id`, `location_id`, `data` y campos opcionales de cliente/metadata. El cuerpo se conserva en los reintentos; la cabecera temporal y su firma V2 se regeneran en cada intento. No reconstruyas JSON ni cambies espacios antes de validar.

## Entrega

Persiste el evento en tu receptor antes de responder 2xx. Usa una restricción única por `body.id` y procesa sus efectos de manera idempotente. Un duplicado ya persistido debe recibir 2xx. No se garantiza orden ni una única recepción. Cualquier 2xx cuenta como aceptación, incluido 202.

Kivly persiste cada entrega antes de enviar. Por defecto realiza un intento inicial y tres reintentos, con al menos 60 segundos entre fallos y timeout de 30 segundos. El trabajador duradero revisa pendientes cada 15 segundos; la cola y la carga pueden retrasar la entrega. Un proceso interrumpido recupera su lease después de dos minutos y puede producir un duplicado si el receptor ya había aceptado.

El evento se encola después de la operación de negocio; no existe una transacción única entre todas las operaciones de negocio y esta cola. Una interrupción en ese intervalo puede perder una notificación. Mantén reconciliación periódica con la API; no uses webhooks como única copia de registros financieros.

## Historial y reintento manual

`GET /api/v1/webhooks/deliveries?page=1&limit=25` admite `delivery_status` (`pending`, `retrying`, `success`, `failed`) y `event`. Devuelve IDs, estado, fechas, intentos, código HTTP y error resumido, sin secretos ni payloads de clientes. Sólo muestra registros con ubicación verificable del nuevo formato; no se reenvía automáticamente el historial antiguo.

`POST /api/v1/webhooks/deliveries/{delivery_id}/retry` devuelve 202 si encola un intento adicional. Sólo permite entregas fallidas del negocio autorizado, con la configuración original vigente. Dos peticiones concurrentes no crean dos reintentos. Una entrega exitosa o en curso produce 409. Se conservan cuerpo, ID del evento, contador de intentos y auditoría del usuario que solicitó el reintento. `delivery_id` es el ID del registro del historial; no es `webhook_id`.

La URL original debe seguir configurada y el evento suscrito. Cambiar a otra URL no autoriza reenviar datos antiguos allí. Se utiliza el secreto actual: al rotarlo, actualiza también el receptor. Actualiza el historial para conocer el resultado real; 202 sólo significa encolado.

## Control de costes

Los eventos agrupados bajo `cost_control` notifican cambios de proveedores, ingredientes, precios, stock, recetas, platos, gastos, objetivos, inversiones, documentos y mermas. Contienen `data.resource_id` y `data.location_id`; consulta el recurso por REST para obtener su estado. Nacen en servicios compartidos por REST, GraphQL y MCP, no sólo en el adaptador público. Las escrituras directas a Mongo o mecanismos que eludan esos servicios no generan automáticamente un evento. Una recálculo o guardado puede volver a notificar el mismo recurso; deduplica por evento y trata la notificación como una invitación a sincronizar el estado actual.

## Facturación y marketing

Consulta [facturación](billing.md) para los eventos `invoice.*`, y [marketing](marketing.md) para `campaign.*`, `automatic_notification.*`, `gift_card.*` y `survey.*`. Sus payloads compactos usan IDs y estados, sin facturas completas, códigos canjeables ni respuestas personales. Todos usan la misma firma, historial por negocio y reintento manual descritos arriba.

## Varios destinos y filtros

Crea destinos en Dashboard → Desarrolladores → Webhooks. Selecciona los eventos, introduce nombre y URL HTTPS y guarda el secreto que se muestra una sola vez. Cada destino firma con su propio secreto y tiene historial y reintentos independientes. Desactivar o borrar un destino impide nuevos envíos y reintentos. Rotar su secreto requiere actualizar el receptor.

Un evento compartido entre destinos conserva el mismo `webhook_id`; cada entrega tiene un ID propio. Para consultar varios tipos utiliza parámetros repetidos: `?event=order.created&event=invoice.issued`. Se devuelve cualquiera de los tipos seleccionados, combinado con los otros filtros. Omitir `event` incluye todos. `endpoint_id` restringe al destino; `legacy` identifica la configuración anterior del negocio.
