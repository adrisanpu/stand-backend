# Pedidos de una web externa al TPV

Usa **`POST /api/v1/orders`**, con un token de integración y permiso `orders:write`, desde el servidor de tu web. El negocio se toma del token; no envíes `location_id`. Este flujo importa una venta de tu canal y crea su cuenta operativa en el TPV. Las rutas `/public/orders/*` son del checkout de Kivly y no forman parte de este contrato para integradores.

## Preparación

Obtén los IDs reales de la sucursal, menú y productos del negocio mediante las lecturas documentadas de `branches`, `food-menus` y `catalog`. La clave necesita los permisos de lectura correspondientes para descubrirlos. El menú debe pertenecer a la sucursal; cada producto debe pertenecer al negocio y estar disponible en esa sucursal. Se admiten productos del catálogo compartido, incluso sin asignación a ese menú. Los IDs ficticios del ejemplo deben reemplazarse.

Esta operación recibe pedidos **inmediatos de recogida o entrega**. Tu canal decide el precio vendido, los gastos de envío y el descuento. No es un checkout que cotice precios, valide cupones, compruebe horarios de venta/zonas de reparto o cobre al cliente. Confirma esas condiciones en tu web antes de importar la venta. Se rechazan sucursales y menús desactivados. No admite pedidos programados, mesas ni combos; las opciones son complementos con nombre y precio, no componentes de combos ni una validación de reglas de modificadores.

## Crear

```http
POST /api/v1/orders
Authorization: Bearer <data.token>
Content-Type: application/json
```

```json
{
  "external_id": "website-2026-001",
  "channel": "my-website",
  "branch_id": "64a000000000000000000002",
  "menu_id": "64a000000000000000000003",
  "order_type": "PICKUP",
  "currency": "EUR",
  "payment": "PAID_EXTERNALLY",
  "customer_name": "Example customer",
  "total_cents": 2600,
  "items": [{
    "menu_item_id": "64a000000000000000000004",
    "name": "Example item",
    "quantity": 2,
    "unit_price_cents": 1200,
    "options": [{"name": "Extra cheese", "unit_price_cents": 100, "quantity": 1}]
  }]
}
```

Todos los campos `_cents` son enteros en centésimas de la moneda del negocio: `2600` son `26.00 EUR`. Los importes incluyen impuestos. El precio base excluye opciones; la cantidad de cada opción se aplica **por unidad del producto**. La igualdad obligatoria es:

`total_cents = Σ(quantity × (unit_price_cents + Σ(option.quantity × option.unit_price_cents))) + shipping_cents − discount_cents`

`shipping_cents` y `discount_cents` valen cero si se omiten. Kivly resuelve el desglose fiscal y los destinos de cocina con su configuración. El nombre del catálogo prevalece sobre `items[].name`. `customer_id` es opcional y, si se incluye, debe ser un cliente del mismo negocio. `customer_name` es obligatorio; no se crea una ficha de cliente automáticamente. Puedes añadir `notes` al pedido y a cada producto.

| `payment` | Uso |
| --- | --- |
| `PAID_EXTERNALLY` | Tu web ya cobró. Kivly registra el pedido como pagado, sin realizar otro cargo. |
| `PAY_ON_COLLECTION` | Recogida (`PICKUP`), pendiente de cobrar en el negocio. |
| `PAY_ON_DELIVERY` | Entrega (`DELIVERY`), pendiente de cobrar al entregar. |

Para `DELIVERY`, añade `customer_phone` y `delivery_address` con `full_address`, `city`, `postal_code` y `country`; `additional_info` es opcional. Admite `shipping_cents`. Una recogida no puede incluir dirección ni gastos de entrega. Esta operación no contrata un repartidor ni inicia un reembolso en el proveedor que cobró en tu web.

Una creación correcta devuelve **201**:

```json
{
  "success": true,
  "replayed": false,
  "data": {
    "id": "64a000000000000000000005",
    "display_id": "KV-123",
    "external_id": "website-2026-001",
    "channel": "my-website",
    "status": "PENDING",
    "payment_status": "PAID",
    "currency": "EUR",
    "total_cents": 2600,
    "pos_order_id": "example-pos-order-id"
  }
}
```

`id` y `pos_order_id` son generados por Kivly. El estado inicial puede ser `ACCEPTED` si el menú acepta automáticamente. Conserva el ID de Kivly junto al de tu tienda. El origen operativo es `api`; `metadata.integration_channel` identifica la web. El pedido sigue los ajustes de aceptación/preparación del negocio y el TPV. Un 201 confirma persistencia y cuenta POS; no confirma que una impresora física haya impreso ni que el establecimiento haya preparado el pedido. El TPV debe tener la versión que reconoce el canal API para el envío automático a cocina.

## Reintentar y consultar

La identidad es **negocio + `channel` + `external_id`**. Usa un identificador estable por pedido de tu tienda y conserva el cuerpo original:

- Repetir el mismo cuerpo devuelve **200**, `replayed: true`, el mismo ID y su estado actual. No crea otro pedido ni otra cuenta, tampoco reabre un pedido terminado.
- Reutilizar esa identidad con un cuerpo diferente devuelve **409**. No cambies la referencia para sortear un timeout.
- **503** puede indicar que el pedido ya está guardado y falta sincronizar el TPV. Repite el cuerpo original respetando `Retry-After`. Tras un timeout, haz lo mismo. Limita los reintentos con espera creciente; si persiste, conserva la referencia y consulta soporte.
- **422** indica importes, campos o referencias incoherentes; **404** un recurso inexistente o ajeno. **401/403** y **429** siguen las reglas generales del kit. No hay un encabezado `Idempotency-Key` genérico.

Consulta `GET /api/v1/orders/{order_id}` y suscribe los eventos `order.*` pertinentes. Utiliza `PATCH /api/v1/orders/{order_id}/status` únicamente para transiciones admitidas por su contrato. Las lecturas históricas y los webhooks usan el esquema del pedido existente: sus importes `total`, `subtotal`, etc. son unidades monetarias decimales, a diferencia de los `_cents` de esta operación. No mezcles ambos formatos.

Prueba en una cuenta destinada a pruebas: creación, aparición en TPV, aceptación, cocina, entrega/cierre, evento recibido, repetición idéntica y conflicto de referencia. Las pruebas locales no sustituyen una comprobación del TPV y sus impresoras conectadas.
