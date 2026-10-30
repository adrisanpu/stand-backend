# Facturación del negocio

Estas operaciones gestionan las facturas que el negocio emite a sus clientes. No gestionan la suscripción del negocio a Kivly, pagos Stripe ni el registro de ventas de Custom Loyalty. Todas las rutas llevan el prefijo `/api/v1` y requieren Bearer del negocio.

## Flujo recomendado

1. Consulta `GET /invoices/` con `page` (desde 1), `limit` (máximo 200), `branch_id`, `order_id`, `display_id`, fechas `created_at_from`/`created_at_to` y filtros `lifecycle_status`, `kind`, `fiscal_effect_status`, `submission_status`. Devuelve `data`, `total`, `page`, `limit`, `totalPages`. Excluye borradores descartados.
2. Crea `POST /invoices/drafts`. Los importes de las líneas son unidades monetarias mayores: `12.50` significa 12,50 €, mientras Custom Loyalty y tarjetas regalo usan unidades menores. Este paso no consume número ni presenta el documento fiscalmente.
3. Consulta `GET /invoices/{invoice_id}/actions`. Usa `PATCH /invoices/drafts/{invoice_id}` para editar o `DELETE` en la misma ruta para descartar antes de emitir.
4. Emite con `POST /invoices/drafts/{invoice_id}/issue`. Consume numeración e invoca el adaptador fiscal configurado. No envíes peticiones de emisión concurrentes ni reintentes automáticamente una respuesta perdida.
5. Consulta `GET /invoices/{invoice_id}` y `GET /invoices/{invoice_id}/pdf`. `pdfUrl` puede ser null. Un PDF no confirma aceptación fiscal.

Ejemplo de borrador, con ID ficticio que debes sustituir por el del contexto:

```json
{
  "locationId": "64a000000000000000000001",
  "customerCompany": {"name": "Cliente de ejemplo", "taxId": "B00000000", "address": "Calle Ejemplo 1"},
  "items": [{"name": "Servicio", "quantity": 1, "totalWithTax": 121, "taxRate": 21}]
}
```

La sociedad emisora y la sucursal deben estar configuradas en el Dashboard. Si se envía `issuerLegalEntityId`, debe coincidir con la sociedad asignada a ese negocio/sucursal. El esquema exacto y los alias camelCase se encuentran en OpenAPI. Las respuestas contienen `success` y `data`, cuyo identificador es `_id`.

No confundas los tres estados: `lifecycleStatus` describe borrador/emisión; `submissionStatus`, la presentación fiscal; `fiscalEffectStatus`, el efecto de sustitución, rectificación o anulación. `GET /actions` devuelve las acciones admitidas en ese momento. Un 409 exige volver a consultar el documento.

## Operaciones posteriores

| Operación | Uso |
| --- | --- |
| `POST /invoices/replacements` | Convertir un ticket en factura completa mediante `invoiceId` o `orderId`, conservando y vinculando los documentos |
| `POST /invoices/rectifications` | Rectificación total o parcial; en la parcial se envían líneas positivas y el servicio aplica el signo corrector |
| `POST /invoices/cancellations` | Anular una emisión errónea sin borrar documento ni liberar número; revisar también `cancellation.submissionResponse` |
| `POST /invoices/{id}/submission/retry` | Reintentar la presentación con la clave fiscal original cuando el estado lo permita |
| `POST /invoices/{id}/submission/refresh` | Consultar y persistir el estado del proveedor fiscal |

Las rutas históricas `POST /invoices/` y `PATCH /invoices/{id}` siguen en el contrato por compatibilidad. Para una nueva integración usa el ciclo de borrador/emisión. Una factura emitida bajo este ciclo no se sobrescribe.

## Webhooks

Eventos: `invoice.draft_created`, `invoice.draft_updated`, `invoice.draft_deleted`, `invoice.issued`, `invoice.updated`, `invoice.replaced`, `invoice.rectified`, `invoice.cancelled`, `invoice.submission_updated`.

`data` contiene `resource_id`, `location_id` y, cuando están disponibles, `lifecycle_status`, `submission_status`, `fiscal_effect_status`, `kind`, `rectifies_invoice_id`, `replaces_invoice_ids`. Estos nombres usan snake_case aunque el documento REST use camelCase. Un evento `invoice.issued` confirma el resultado del servicio de emisión, no necesariamente la aceptación de la autoridad. Sustituciones y rectificaciones notifican el ID del nuevo documento y sus vínculos.

Lee el estado actual por REST al recibirlos. La operación de negocio y el encolado no forman una transacción única; conserva reconciliación periódica y no uses los webhooks como única copia contable. Firma, historial y reintento manual: [webhooks](webhooks.md).
