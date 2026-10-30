# Custom Loyalty API integration

Contrato para conectar un TPV, ERP o aplicación con la fidelización de Kivly.
Las dos operaciones principales conservan los nombres de la documentación histórica:

| Operación | Ruta | operationId |
| --- | --- | --- |
| Process invoice and validate customer | `POST /api/v1/loyalty/{location_id}` | `processInvoice` |
| Get customer loyalty information | `GET /api/v1/loyalty/customer/{location_id}/{identifier}` | `getCustomerLoyalty` |

Usa `https://api.getkivly.com`. Obtén `data.token` con las credenciales del negocio
en `POST /api/v1/auth/token`, siguiendo [la guía de autenticación](integration-guide.md).
Envía `Authorization: Bearer <token>`. El `location_id` debe coincidir con el negocio
del token. Estas operaciones comparten los límites de la Public API; respeta
`Retry-After` cuando recibas 429. No uses un token de cliente como credencial de integración.

## 1. Consultar al cliente

Busca por ID externo, email o teléfono, codificando el identificador como un segmento
de URL (`encodeURIComponent` en JavaScript o `urllib.parse.quote(value, safe='')`
en Python). Incluye el prefijo internacional del teléfono. La ruta prueba ID
externo, email y teléfono; el servicio heredado también prueba `personal_id` cuando
la búsqueda inicial no resuelve un cliente.

La respuesta contiene `loyalty`, `promotions` y `user` directamente, sin `data`.
Conserva `user.id`, el ID de Kivly, para enviar después la venta. Consulta los
esquemas y ejemplos completos con:

```sh
python scripts/inspect_api.py operation GET '/api/v1/loyalty/customer/{location_id}/{identifier}'
```

- `loyalty.balance`: equivalente monetario en unidades menores; 625 equivale a
  6,25 EUR en un negocio EUR. En VISIT vale cero.
- `loyalty.points`: saldo bruto en puntos (POINT) o unidades monetarias menores
  (MONEY). En VISIT se devuelve `loyalty.visits`.
- `tier.id`: ID de nivel o el número `0` si no hay nivel.
- `promotions`: opciones ofrecidas, con fechas Unix en segundos. Interpreta
  `discount_amount` según `type` y `reward_type`; no deduzcas requisitos de
  producto a partir de un descuento de cero. La opción sintética `balance_*`
  no es un ID de recompensa normal: no la envíes como si lo fuera en `promotions`
  del POST de venta, cuyo procesamiento de recompensas exige IDs ObjectId.

El contrato heredado puede devolver HTTP 200 para un cliente no encontrado o
algunos errores internos, con saldo cero, promociones vacías y datos de usuario
nulos. No interpretes el identificador repetido en `user.id` como un cliente
registrado. También contempla la respuesta alternativa
`{"MemberId":"unknown","DisplayText":"…","Rewards":[]}`. Esos casos no
confirman saldo ni habilitan un canje. Esta consulta no crea clientes.

## 2. Enviar una venta

Envía JSON con `name`, `total` y `customer`. Aunque el modelo permite omitir
`items`, la validación de negocio exige al menos un artículo. `customer.id` y
`customer.name` son obligatorios. El ID de Kivly procede de la consulta anterior;
un identificador externo se envía en `customer.external_id` o
`external_customer_id`. El POST busca por ID externo, teléfono y finalmente ID
de Kivly: **no busca por email** ni registra automáticamente al cliente.

Ejemplo sintético: sustituye el cliente y el producto por los de tu cuenta de pruebas.

```json
{
  "name": "SALE-1001",
  "code": "SALE-1001",
  "source": "custom-pos",
  "total": {"total": 1250, "subtotal": 1250, "discount": 0, "tax": 0, "currency": "EUR"},
  "customer": {"id": "64a000000000000000000003", "name": "Example Customer"},
  "items": [{"id": "product-1", "name": "Example product", "quantity": 1, "price": 1250, "type": "product"}]
}
```

`total.total`, `subtotal`, `discount` y `items[].price` son enteros en unidades
monetarias menores. `total.tax` es un porcentaje entero de IVA, no un importe.
La configuración de IVA y fidelización del negocio determina lo que se acumula.
La operación espera al procesamiento antes de responder; no es una aceptación
de trabajo pendiente en segundo plano ni emite una factura fiscal.

Comprueba `success` y `status`, además del estado HTTP:

- Aceptada: `success: true`, `status: "accepted"`, `points_earned` y
  `available_balance` actualizado en unidades brutas (puntos, céntimos o visitas).
  Los importes del ejemplo de respuesta dependen de la configuración del negocio.
- Duplicada: añade `duplicate: true` y devuelve `points_earned: 0`.
- Rechazada: `success: false`, `status: "rejected"`, `message`, incluso con HTTP 200.
- JSON/campos inválidos: 422; la respuesta puede contener `detail` o `errors`.
- Fallo durante procesamiento: 500. No presupongas que no hubo efectos parciales.

Usa un `code` estable y único por venta dentro del negocio. La detección de una
transacción existente no es una garantía de ejecución única ante concurrencia.
Además, las promociones se procesan antes de esa comprobación: reenviar una venta
con promociones no está garantizado como inocuo. Ante timeout o fallo ambiguo,
concilia el resultado antes de repetir; no reintentes escrituras automáticamente.

## Prueba de integración

En una cuenta de pruebas autorizada, consulta un cliente registrado, envía una
venta sin promociones, comprueba aceptación y saldo, y vuelve a consultar al
cliente. Prueba el código duplicado secuencialmente y un cliente desconocido.
Simula 401/403/429, errores de red y respuestas de rechazo en tu cliente sin
generar carga real. Prueba los canjes por separado antes de enviar promociones.
Un ejemplo validado contra el esquema no sustituye estas pruebas funcionales.
