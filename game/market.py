"""坊市（交易行）：玩家间物品交易核心逻辑。"""

from __future__ import annotations

import logging
import time
import uuid
from typing import TYPE_CHECKING

from .constants import ITEM_REGISTRY, EQUIPMENT_REGISTRY, parse_stored_heart_method_item_id
from .data_manager import DataManager

if TYPE_CHECKING:
    from .models import Player

logger = logging.getLogger(__name__)

# ── 常量 ────────────────────────────────────────────────
LISTING_DURATION = 86400  # 24 小时
MIN_FEE_RATE = 0.01
MAX_FEE_RATE = 0.03
DEFAULT_FEE_RATE = 0.02
MAX_LISTINGS_PER_PLAYER = 10
MIN_UNIT_PRICE = 1


# ── 手续费计算 ──────────────────────────────────────────

def calculate_listing_fee(
    unit_price: int,
    quantity: int,
    market_stats: dict,
) -> tuple[int, float]:
    """计算上架手续费。

    返回 (fee, rate)。
    """
    total = unit_price * quantity
    count = market_stats.get("count", 0)
    avg_price = market_stats.get("avg_price", 0)

    if count == 0 or avg_price <= 0:
        rate = DEFAULT_FEE_RATE
    else:
        price_ratio = unit_price / avg_price
        if price_ratio > 1.5:
            rate = 0.03
        elif price_ratio >= 1.2:
            rate = 0.025
        elif price_ratio >= 0.8:
            rate = 0.01
        elif price_ratio >= 0.5:
            rate = 0.015
        else:
            rate = 0.02

        # 成交量修正：高交易量降低费率，低交易量提高费率
        total_qty = market_stats.get("total_quantity", 0)
        if total_qty >= 50:
            rate -= 0.005
        elif total_qty <= 5:
            rate += 0.005

    rate = max(MIN_FEE_RATE, min(MAX_FEE_RATE, rate))
    fee = max(1, int(total * rate))
    return fee, rate


# ── 上架 ────────────────────────────────────────────────

async def list_item(
    player: Player,
    item_id: str,
    quantity: int,
    unit_price: int,
    dm: DataManager,
) -> dict:
    """上架物品到坊市。

    调用方应传入玩家快照（非共享对象），并在外层持有玩家锁。
    事务成功后由调用方将快照回写到共享对象。
    """
    # 临时心法道具绑定过期时间，禁止流转
    if parse_stored_heart_method_item_id(item_id):
        return {"success": False, "message": "临时心法道具不可上架"}

    # 校验物品存在
    item_def = ITEM_REGISTRY.get(item_id)
    equip_def = EQUIPMENT_REGISTRY.get(item_id)
    if not item_def and not equip_def:
        return {"success": False, "message": "物品不存在"}

    item_name = item_def.name if item_def else equip_def.name

    # 校验库存
    owned = player.inventory.get(item_id, 0)
    if owned < quantity:
        return {"success": False, "message": f"背包中只有 {owned} 个「{item_name}」，不够上架"}

    # 校验价格
    if unit_price < MIN_UNIT_PRICE:
        return {"success": False, "message": f"单价不能低于 {MIN_UNIT_PRICE} 灵石"}

    if quantity < 1:
        return {"success": False, "message": "数量不能小于 1"}

    # 校验上架数未超上限
    my_listings = await dm.get_my_listings(player.user_id)
    active_count = sum(1 for l in my_listings if l["status"] == "active")
    if active_count >= MAX_LISTINGS_PER_PLAYER:
        return {"success": False, "message": f"上架数已达上限（{MAX_LISTINGS_PER_PLAYER}件）"}

    # 计算手续费
    stats = await dm.get_market_stats(item_id)
    fee, rate = calculate_listing_fee(unit_price, quantity, stats)

    if player.spirit_stones < fee:
        return {
            "success": False,
            "message": f"灵石不足以支付手续费（需 {fee} 灵石，当前 {player.spirit_stones}）",
        }

    total_price = unit_price * quantity
    now = time.time()
    listing_id = uuid.uuid4().hex[:12]

    # 在快照上修改状态（不影响共享对象）
    player.inventory[item_id] = owned - quantity
    if player.inventory[item_id] <= 0:
        del player.inventory[item_id]
    player.spirit_stones -= fee

    # 在独立连接事务内原子完成：上架记录 + 卖家资产扣除
    try:
        async with dm.transaction() as tx:
            await tx.execute(
                """INSERT INTO market_listings
                   (listing_id, seller_id, item_id, quantity, unit_price,
                    total_price, fee, listed_at, expires_at, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    listing_id,
                    player.user_id,
                    item_id,
                    quantity,
                    unit_price,
                    total_price,
                    fee,
                    now,
                    now + LISTING_DURATION,
                    "active",
                ),
            )
            await dm._upsert_player(player, db=tx)

    except Exception as e:
        logger.error(f"坊市上架数据库操作失败: {e}")
        return {"success": False, "message": "上架失败，请稍后重试"}

    return {
        "success": True,
        "message": (
            f"成功上架「{item_name}」x{quantity}，"
            f"单价 {unit_price} 灵石，总价 {total_price} 灵石，"
            f"手续费 {fee} 灵石（{rate*100:.1f}%），"
            f"编号 {listing_id[:6]}"
        ),
        "listing_id": listing_id,
        "fee": fee,
        "rate": rate,
        "players_saved": True,
    }


# ── 购买 ────────────────────────────────────────────────

async def buy_item(
    buyer: Player,
    seller: Player,
    listing_id: str,
    dm: DataManager,
) -> dict:
    """从坊市购买物品。

    调用方应传入买家和卖家的快照（非共享对象），并在外层持有双方锁。
    事务成功后由调用方将快照回写到共享对象。
    """
    listing = await dm.get_listing_by_id(listing_id)
    if not listing:
        return {"success": False, "message": "该商品不存在"}

    if listing["status"] != "active":
        return {"success": False, "message": "该商品已售出或已下架"}

    now = time.time()
    if listing["expires_at"] <= now:
        return {"success": False, "message": "该商品已过期"}

    if listing["seller_id"] == buyer.user_id:
        return {"success": False, "message": "不能购买自己的商品"}

    if listing["seller_id"] != seller.user_id:
        return {"success": False, "message": "卖家信息异常，请稍后重试"}

    total_price = listing["total_price"]
    if buyer.spirit_stones < total_price:
        return {
            "success": False,
            "message": f"灵石不足（需 {total_price}，当前 {buyer.spirit_stones}）",
        }

    item_id = listing["item_id"]
    item_def = ITEM_REGISTRY.get(item_id)
    equip_def = EQUIPMENT_REGISTRY.get(item_id)
    item_name = (item_def.name if item_def else equip_def.name) if (item_def or equip_def) else item_id

    quantity = int(listing["quantity"])

    # 在快照上修改状态（不影响共享对象）
    buyer.spirit_stones -= total_price
    buyer.inventory[item_id] = buyer.inventory.get(item_id, 0) + quantity
    seller.spirit_stones += total_price

    # 在独立连接事务内原子完成所有数据库变更
    now = time.time()
    try:
        async with dm.transaction() as tx:
            cur = await tx.execute(
                """UPDATE market_listings
                   SET status = ?, buyer_id = ?, sold_at = ?
                   WHERE listing_id = ? AND status = ?""",
                ("sold", buyer.user_id, now, listing_id, "active"),
            )
            if cur.rowcount <= 0:
                raise DataManager.TransactionAbort("该商品已被其他修士抢先购买")

            await tx.execute(
                """INSERT INTO market_history
                   (history_id, item_id, quantity, unit_price, total_price,
                    fee, seller_id, buyer_id, sold_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    uuid.uuid4().hex[:12],
                    item_id,
                    quantity,
                    listing["unit_price"],
                    total_price,
                    listing["fee"],
                    seller.user_id,
                    buyer.user_id,
                    now,
                ),
            )

            # 买卖双方全量 upsert 快照（双方均受锁保护，无并发风险）
            await dm._upsert_player(buyer, db=tx)
            await dm._upsert_player(seller, db=tx)

    except DataManager.TransactionAbort as e:
        return {"success": False, "message": str(e)}
    except Exception as e:
        logger.error(f"坊市交易数据库操作失败: {e}")
        return {"success": False, "message": "交易失败，请稍后重试"}

    return {
        "success": True,
        "message": f"成功购买「{item_name}」x{listing['quantity']}，花费 {total_price} 灵石",
        "seller_id": seller.user_id,
        "players_saved": True,
    }


# ── 下架 ────────────────────────────────────────────────

async def cancel_listing(
    player: Player,
    listing_id: str,
    dm: DataManager,
) -> dict:
    """取消上架（手续费不退）。

    调用方应传入玩家快照（非共享对象），并在外层持有玩家锁。
    事务成功后由调用方将快照回写到共享对象。
    """
    listing = await dm.get_listing_by_id(listing_id)
    if not listing:
        return {"success": False, "message": "该商品不存在"}

    if listing["seller_id"] != player.user_id:
        return {"success": False, "message": "这不是你的商品"}

    if listing["status"] != "active":
        return {"success": False, "message": "该商品已不在架上"}

    item_id = listing["item_id"]
    item_def = ITEM_REGISTRY.get(item_id)
    equip_def = EQUIPMENT_REGISTRY.get(item_id)
    item_name = (item_def.name if item_def else equip_def.name) if (item_def or equip_def) else item_id

    # 在快照上修改状态
    player.inventory[item_id] = player.inventory.get(item_id, 0) + listing["quantity"]

    # 在独立连接事务内原子完成：下架状态更新 + 玩家物品退回
    try:
        async with dm.transaction() as tx:
            cur = await tx.execute(
                """UPDATE market_listings SET status = ?
                   WHERE listing_id = ? AND status = ?""",
                ("cancelled", listing_id, "active"),
            )
            if cur.rowcount <= 0:
                return {"success": False, "message": "该商品状态已变更，请刷新后重试"}

            await dm._upsert_player(player, db=tx)

    except Exception as e:
        logger.error(f"坊市下架数据库操作失败: {e}")
        return {"success": False, "message": "下架失败，请稍后重试"}

    return {
        "success": True,
        "message": f"已下架「{item_name}」x{listing['quantity']}，物品已退回（手续费不退）",
        "players_saved": True,
    }


# ── 查询 ────────────────────────────────────────────────

async def get_listings(
    dm: DataManager,
    page: int = 1,
    page_size: int = 20,
) -> dict:
    """分页获取活跃商品列表。"""
    return await dm.get_active_listings(page, page_size)


async def get_my_listings(player: Player, dm: DataManager) -> list[dict]:
    """获取自己的上架记录。"""
    return await dm.get_my_listings(player.user_id)


async def clear_my_history(
    player: Player,
    dm: DataManager,
    include_expired: bool = False,
) -> dict:
    """清理自己的历史上架记录（已售/已下架，可选含已过期）。"""
    deleted = await dm.clear_my_listing_history(
        player.user_id,
        include_expired=bool(include_expired),
    )
    scope = "已售/已下架/已过期" if include_expired else "已售/已下架"
    return {
        "success": True,
        "deleted": int(deleted or 0),
        "message": f"已清理{scope}记录 {int(deleted or 0)} 条",
    }


# ── 过期清理 ────────────────────────────────────────────

async def cleanup_expired(
    dm: DataManager,
) -> list[dict]:
    """查询所有过期但仍为 active 的商品列表，不做任何状态变更。

    实际的过期处理（标记 expired + 退回物品）由 engine 层逐条
    在持有玩家锁的事务内原子完成。
    """
    now = time.time()
    return await dm.get_expired_active_listings(now)


def get_item_name(item_id: str) -> str:
    """根据 item_id 获取显示名称。"""
    item_def = ITEM_REGISTRY.get(item_id)
    if item_def:
        return item_def.name
    equip_def = EQUIPMENT_REGISTRY.get(item_id)
    if equip_def:
        return equip_def.name
    return item_id
