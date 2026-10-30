# Campañas, automatizaciones, tarjetas y encuestas

Todas las rutas de esta guía llevan `/api/v1` y Bearer del negocio. Las listas nuevas usan `skip` desde 0 y `limit` (25 por defecto, máximo 100), y devuelven `data`, `skip`, `limit`, `has_next`, salvo el listado de notificaciones automáticas, que conserva `GeneralResponse`. Las respuestas de recurso usan `{success:true,data:...}`; consulta los alias y campos exactos en OpenAPI. Las operaciones pueden activar costes, recompensas y envíos reales: una cuenta de prueba en el panel normal sigue usando producción.

## Campañas multicanal

Usa `GET/POST /campaigns`, `GET/PATCH/DELETE /campaigns/{id}` y `GET /campaigns/{id}/statistics`. Son campañas de email, push, SMS y WhatsApp. No son las promociones de fidelización ni las campañas específicas de Instagram.

`POST /campaigns` crea un borrador sin enviar. Ejemplo mínimo de push:

```json
{
  "name": "Novedades de la semana",
  "channels": ["push"],
  "channel_config": {"push": {"title": "Novedades", "message": "Consulta nuestra carta"}},
  "scheduling": {"type": "immediate"},
  "segmentation": {"type": "all"}
}
```

Cada canal seleccionado necesita configuración. Para email, elige una plantilla de `GET /campaigns/email-templates`; para segmentación, consulta `GET /campaigns/segments` o las listas de contactos. WhatsApp requiere una plantilla y conexión válidas del proveedor. La audiencia real depende de consentimientos, disponibilidad del canal y reglas del servicio; una estimación no garantiza entrega.

`POST /campaigns/{id}/send` acepta un borrador; `202` significa aceptado, no entregado. Para programar, usa `scheduling.type=scheduled`, `scheduledDate`, `scheduledTime` y una zona IANA en `timezone`; sin zona explícita se usa la del negocio y después Europe/Madrid como fallback. Hay rutas POST `/pause`, `/resume` y `/cancel`; sólo operan en los estados admitidos por el servicio. Pausar/cancelar no retira mensajes enviados. Sólo pueden editarse borradores/pausadas y borrarse borradores por REST.

El servicio reclama atómicamente el estado antes de despachar para rechazar dos aceptaciones concurrentes. No existe garantía universal de envío exactamente una vez: ante timeout, consulta campaña/estadísticas antes de repetir. Si la programación falla después de reclamar el estado, requiere reconciliación. Los cargos dependen del canal y configuración del negocio, no del HTTP 202.

Eventos: `campaign.created`, `campaign.updated`, `campaign.deleted`, `campaign.status_changed`, `campaign.results_updated`. Los eventos de resultados representan persistencia de métricas en ese servicio; no prometen un webhook por cada recibo del proveedor.

## Notificaciones automáticas

`GET/POST /automations/notifications`, `GET/PATCH/DELETE /automations/notifications/{id}`, `PUT /automations/notifications/{id}/status` con `{"is_active":true}` y `GET /automations/notifications/{id}/history`.

Son notificaciones a clientes activadas por eventos, no la bandeja administrativa del propietario. Se crean desactivadas por defecto. Configura `type`, `name`, `description`, `channels` y `channel_configs`, y opcionalmente segmento, recompensa, temporización y cooldown. Los tipos y configuraciones admitidos están en los esquemas `AutomationWrite` y `DefaultNotification`. El servicio admite una configuración no eliminada por tipo y negocio; desactivarla no permite duplicar ese tipo.

Activar permite futuros envíos cuando coincida el disparador. El historial muestra intentos registrados y resultados por canal; no todo envío aceptado por un proveedor significa que el cliente lo recibió. Usa paginación y evita almacenar cuerpos completos en logs de tu integración.

Eventos: `automatic_notification.created`, `automatic_notification.updated`, `automatic_notification.deleted`, `automatic_notification.status_changed`, `automatic_notification.delivery_recorded`. Este último identifica la fila del historial en `resource_id` y su configuración en `notification_id`; significa intento registrado, no entrega final confirmada.

## Tarjetas regalo

`GET/POST /gift-cards`, `GET/PATCH/DELETE /gift-cards/{id}` y `GET /gift-cards/statistics` gestionan el producto. `price_cents`, `balance_cents`, `current_balance_cents`, `amount_cents` y los ingresos expresados en céntimos usan unidades monetarias menores. `DELETE` desactiva el producto conservando tarjetas emitidas y genera `gift_card.updated`.

`POST /gift-cards/{id}/instances` emite una tarjeta canjeable desde un producto activo; acepta `customer_id` y datos de regalo. **No cobra un pago**: la aplicación debe resolver el cobro antes de emitir. Puede crear pases Wallet. El endpoint no envía por sí mismo un email al destinatario.

Consulta `GET /gift-cards/instances`, `GET /gift-cards/instances/{id}` o `POST /gift-cards/instances/lookup` con `{"code":"..."}`. El código permite canjear: trátalo como dato sensible y no lo pongas en URL, analítica o logs. La API devuelve enlaces Wallet utilizables, pero excluye tokens internos de autenticación y dispositivos.

`POST /gift-cards/instances/redeem` recibe `code`, opcionalmente `amount_cents` positivo para saldo o `uses` positivo (por defecto 1), y `notes`. **Omitir amount_cents consume todo el saldo monetario restante**. Valida estado, vigencia y saldo/usos. El canje compara el saldo leído en una actualización atómica; una modificación concurrente puede producir 409. Consulta y concilia antes de volver a canjear: no hay `Idempotency-Key` universal.

Eventos: `gift_card.created`, `gift_card.updated`, `gift_card.deleted` (borrado por otros flujos del servicio), `gift_card.instance_created`, `gift_card.instance_redeemed`. Los dos últimos llevan `gift_card_id` y el ID de la tarjeta emitida en `resource_id`, nunca su código ni tokens Wallet.

## Encuestas

`GET/POST /surveys`, `GET/PATCH/DELETE /surveys/{id}`, `GET /surveys/{id}/responses`, `GET /surveys/{id}/statistics` y `POST /surveys/responses`.

Crea con `location_id` del contexto, título, preguntas y recompensa. Define explícitamente `is_active` y `allow_notification`: ambos son true por defecto en el modelo existente y publicar puede activar el flujo de notificaciones. Usa `reward.type=NONE` si no quieres conceder recompensa. Los otros tipos y sus unidades dependen del sistema de fidelización; no inventes una conversión de puntos a dinero.

Después de crear, consulta la encuesta para obtener IDs asignados de preguntas/opciones. Envía `survey_id`, `customer_id` opcional y `answers` con `question_id` y `value`, `rating` o `product_id` según la pregunta. Los productos de una pregunta pueden ser opciones personalizadas, no IDs del catálogo. Se comprueban preguntas obligatorias, opciones conocidas y rangos; el servicio también exige encuesta activa, fechas válidas y límites de repetición. Las respuestas anónimas no conceden recompensa. Repetir una petición puede tener efectos: concilia el historial ante un timeout.

Actualizar las preguntas regenera sus IDs en el servicio existente: vuelve a leer la encuesta antes de presentar o enviar un formulario actualizado. No mezcles respuestas de versiones distintas.

Eventos: `survey.created`, `survey.updated`, `survey.deleted`, `survey.response_submitted`. El último contiene `survey_id` e ID de respuesta en `resource_id`; las respuestas personales se consultan por REST y no se incluyen en el evento compacto.

## Recepción de eventos

Estos eventos nacen en los servicios compartidos, también cuando los utiliza el Dashboard. Las escrituras que los eluden no generan automáticamente eventos. Todos llevan `data.resource_id` y `data.location_id`; los hijos añaden su ID padre. Aplica [firma, deduplicación, reconciliación, historial y reintentos](webhooks.md). Una notificación puede llegar duplicada o fuera de orden y no sustituye la lectura del estado actual.
