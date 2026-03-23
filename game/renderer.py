"""Pillow 图片渲染器：绘制面板/帮助/排行/背包等图片。"""

from __future__ import annotations

import io
import os
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

# 颜色定义
BG_COLOR = (10, 10, 20)
CARD_BG = (18, 18, 42)
BORDER_COLOR = (74, 58, 138)
TEXT_PRIMARY = (232, 224, 208)
TEXT_SECONDARY = (168, 152, 120)
TEXT_MUTED = (104, 88, 72)
ACCENT_GOLD = (212, 164, 64)
ACCENT_CYAN = (64, 176, 192)
ACCENT_RED = (192, 64, 64)
ACCENT_GREEN = (64, 160, 96)
ACCENT_PURPLE = (138, 92, 207)
BAR_BG = (30, 30, 50)

# 字体 - 优先加载项目自带字体，其次系统中文字体
_FONT_CACHE: dict[int, ImageFont.FreeTypeFont] = {}
_PLUGIN_FONT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "fonts", "DouyinSansBold.otf")


def _get_font(size: int) -> ImageFont.FreeTypeFont:
    if size in _FONT_CACHE:
        return _FONT_CACHE[size]
    # 优先使用项目自带的抖音字体
    if os.path.exists(_PLUGIN_FONT):
        try:
            font = ImageFont.truetype(_PLUGIN_FONT, size)
            _FONT_CACHE[size] = font
            return font
        except Exception:
            pass
    # 回退到系统字体
    font_paths = [
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/wqy-microhei/wqy-microhei.ttc",
        "C:/Windows/Fonts/msyh.ttc",
        "C:/Windows/Fonts/simhei.ttf",
        "/System/Library/Fonts/PingFang.ttc",
    ]
    for path in font_paths:
        if os.path.exists(path):
            try:
                font = ImageFont.truetype(path, size)
                _FONT_CACHE[size] = font
                return font
            except Exception:
                continue
    font = ImageFont.load_default()
    _FONT_CACHE[size] = font
    return font


def _draw_rounded_rect(draw: ImageDraw.ImageDraw, xy, fill, radius=10):
    """绘制圆角矩形。"""
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=BORDER_COLOR, width=1)


def render_panel(data: dict) -> bytes:
    """绘制角色面板图片。"""
    # 计算加成信息，确定是否需要额外空间
    base_atk = data.get("attack", 0)
    base_def = data.get("defense", 0)
    equip_bonus = data.get("equip_bonus") or {}
    equip_atk = equip_bonus.get("attack", 0)
    equip_def = equip_bonus.get("defense", 0)
    hm_info = data.get("heart_method_info")
    hm_bonus = hm_info.get("bonus", {}) if hm_info else {}
    hm_atk = hm_bonus.get("attack_bonus", 0)
    hm_def = hm_bonus.get("defense_bonus", 0)
    has_bonus = (equip_atk + equip_def + hm_atk + hm_def) > 0

    w = 520
    h = 540 if has_bonus else 500
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(24)
    font_main = _get_font(16)
    font_small = _get_font(13)
    font_label = _get_font(14)
    font_bonus = _get_font(11)

    def _clip(text: str, max_chars: int) -> str:
        s = str(text or "")
        return s if len(s) <= max_chars else s[:max_chars - 1] + "…"

    # 标题
    name = data.get("name", "未知")
    draw.text((w // 2, 26), name, fill=ACCENT_GOLD, font=font_title, anchor="mt")
    draw.text((w // 2, 58), data.get("realm_name", "未知"), fill=ACCENT_CYAN, font=font_main, anchor="mt")

    # 分割线
    draw.line((24, 86, w - 24, 86), fill=BORDER_COLOR, width=1)

    # 基础属性
    draw.text((30, 98), "基础属性", fill=TEXT_SECONDARY, font=font_label)
    exp = data.get("exp", 0)
    exp_next = data.get("exp_to_next", 0)

    total_atk = data.get("total_attack", base_atk)
    total_def = data.get("total_defense", base_def)

    base_rows = [
        ("经验", f"{exp}/{exp_next}"),
        ("生命", f"{data.get('hp', 0)}/{data.get('max_hp', 0)}"),
        ("攻击", str(total_atk)),
        ("防御", str(total_def)),
        ("灵石", str(data.get("spirit_stones", 0))),
        ("灵气", str(data.get("lingqi", 0))),
        ("道韵", str(data.get("dao_yun", 0))),
        ("心法值", str(data.get("heart_method_value", 0))),
    ]
    left_x = 30
    right_x = w // 2 + 8
    start_y = 126
    row_h = 34
    for i in range(0, len(base_rows), 2):
        k1, v1 = base_rows[i]
        draw.text((left_x, start_y), k1, fill=TEXT_SECONDARY, font=font_main)
        draw.text((left_x + 180, start_y), v1, fill=TEXT_PRIMARY, font=font_main, anchor="rt")
        if i + 1 < len(base_rows):
            k2, v2 = base_rows[i + 1]
            draw.text((right_x, start_y), k2, fill=TEXT_SECONDARY, font=font_main)
            draw.text((w - 30, start_y), v2, fill=TEXT_PRIMARY, font=font_main, anchor="rt")
        start_y += row_h

        # 攻击/防御行后追加加成明细
        if i == 2 and has_bonus:
            parts_atk = [f"基础{base_atk}"]
            if equip_atk:
                parts_atk.append(f"+装备{equip_atk}")
            if hm_atk:
                parts_atk.append(f"+心法{hm_atk}")
            parts_def = [f"基础{base_def}"]
            if equip_def:
                parts_def.append(f"+装备{equip_def}")
            if hm_def:
                parts_def.append(f"+心法{hm_def}")
            detail_atk = " ".join(parts_atk)
            detail_def = " ".join(parts_def)
            draw.text((left_x + 4, start_y - 16), detail_atk, fill=ACCENT_GREEN, font=font_bonus)
            draw.text((right_x + 4, start_y - 16), detail_def, fill=ACCENT_GREEN, font=font_bonus)
            start_y += 18

    # 修行配置
    split_y = start_y + 6
    draw.line((24, split_y, w - 24, split_y), fill=BORDER_COLOR, width=1)
    cfg_title_y = split_y + 12
    draw.text((30, cfg_title_y), "修行配置", fill=TEXT_SECONDARY, font=font_label)
    equip_rows = [
        ("心法", _clip(data.get("heart_method_name", "无"), 14)),
        ("武器", _clip(data.get("weapon_name", "无"), 14)),
        ("功法一", _clip(data.get("gongfa_1", "无"), 14)),
        ("功法二", _clip(data.get("gongfa_2", "无"), 14)),
        ("功法三", _clip(data.get("gongfa_3", "无"), 14)),
        ("护甲", _clip(data.get("armor_name", "无"), 14)),
    ]
    y = cfg_title_y + 30
    for i in range(0, len(equip_rows), 2):
        k1, v1 = equip_rows[i]
        draw.text((left_x, y), k1, fill=TEXT_SECONDARY, font=font_main)
        draw.text((left_x + 180, y), v1, fill=ACCENT_GOLD, font=font_main, anchor="rt")
        if i + 1 < len(equip_rows):
            k2, v2 = equip_rows[i + 1]
            draw.text((right_x, y), k2, fill=TEXT_SECONDARY, font=font_main)
            draw.text((w - 30, y), v2, fill=ACCENT_GOLD, font=font_main, anchor="rt")
        y += row_h

    # 底部提示
    draw.text((w // 2, h - 24), "修仙世界", fill=TEXT_MUTED, font=font_small, anchor="mt")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_help(commands: list[tuple[str, str]], prefix: str = "修仙") -> bytes:
    """绘制帮助指令列表图片。"""
    line_h = 36
    padding = 20
    h = padding * 2 + 50 + len(commands) * line_h + 10
    w = 440
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(20)
    font_cmd = _get_font(15)
    font_desc = _get_font(12)

    draw.text((w // 2, 25), "修仙世界 · 指令", fill=ACCENT_GOLD, font=font_title, anchor="mt")

    y = 55
    for cmd, desc in commands:
        draw.text((30, y), f"/{prefix} {cmd}", fill=ACCENT_CYAN, font=font_cmd)
        draw.text((30, y + 18), f"  {desc}", fill=TEXT_MUTED, font=font_desc)
        y += line_h

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_ranking(rankings: list[dict], total: int, online: int,
                   my_rank: Optional[dict] = None) -> bytes:
    """绘制排行榜图片。"""
    count = min(len(rankings), 10)
    line_h = 28
    padding = 20
    h = padding * 2 + 60 + count * line_h + (40 if my_rank else 0) + 10
    w = 400
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(18)
    font_main = _get_font(14)
    font_small = _get_font(12)

    draw.text((w // 2, 22), "修仙排行榜", fill=ACCENT_GOLD, font=font_title, anchor="mt")
    draw.text((30, 48), f"总修士: {total}", fill=TEXT_MUTED, font=font_small)
    draw.text((w - 30, 48), f"在线: {online}", fill=TEXT_MUTED, font=font_small, anchor="rt")

    y = 70
    for i, r in enumerate(rankings[:10]):
        rank = r.get("rank", i + 1)
        color = ACCENT_GOLD if rank <= 3 else TEXT_PRIMARY
        draw.text((30, y), f"{rank}.", fill=color, font=font_main)
        draw.text((60, y), r.get("name", ""), fill=color, font=font_main)
        draw.text((w - 30, y), r.get("realm_name", ""), fill=ACCENT_CYAN, font=font_small, anchor="rt")
        y += line_h

    if my_rank and my_rank.get("rank", 0) > 10:
        y += 6
        draw.line((30, y, w - 30, y), fill=BORDER_COLOR, width=1)
        y += 8
        draw.text((30, y), f"{my_rank['rank']}.", fill=ACCENT_CYAN, font=font_main)
        draw.text((60, y), "你", fill=ACCENT_CYAN, font=font_main)
        draw.text((w - 30, y), my_rank.get("realm_name", ""), fill=ACCENT_CYAN, font=font_small, anchor="rt")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_death_ranking(rankings: list[dict]) -> bytes:
    """绘制死亡排行榜图片。"""
    count = min(len(rankings), 10)
    line_h = 28
    padding = 20
    h = padding * 2 + 50 + max(count, 1) * line_h + 10
    w = 380
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(18)
    font_main = _get_font(14)
    font_small = _get_font(12)

    draw.text((w // 2, 22), "死亡排行榜", fill=ACCENT_RED, font=font_title, anchor="mt")

    y = 54
    if count <= 0:
        draw.text((w // 2, y), "暂无死亡记录", fill=TEXT_MUTED, font=font_main, anchor="mt")
    else:
        for i, r in enumerate(rankings[:10]):
            rank = r.get("rank", i + 1)
            name = r.get("name", "")
            deaths = r.get("death_count", 0)
            realm_name = r.get("realm_name", "")
            color = ACCENT_GOLD if rank <= 3 else TEXT_PRIMARY
            draw.text((24, y), f"{rank}.", fill=color, font=font_main)
            draw.text((56, y), name, fill=color, font=font_main)
            draw.text((w - 24, y), f"{deaths}次", fill=ACCENT_RED, font=font_small, anchor="rt")
            draw.text((w - 24, y + 15), realm_name, fill=ACCENT_CYAN, font=font_small, anchor="rt")
            y += line_h

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_inventory(items: list[dict]) -> bytes:
    """绘制背包图片。"""
    if not items:
        items = []
    line_h = 30
    padding = 20
    h = padding * 2 + 50 + max(len(items), 1) * line_h + 10
    w = 400
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(18)
    font_main = _get_font(14)
    font_desc = _get_font(11)

    draw.text((w // 2, 22), "背 包", fill=ACCENT_GOLD, font=font_title, anchor="mt")

    y = 55
    if not items:
        draw.text((w // 2, y), "背包为空", fill=TEXT_MUTED, font=font_main, anchor="mt")
    else:
        for item in items:
            name = item.get("name", "?")
            count = item.get("count", 0)
            desc = item.get("description", "")
            draw.text((30, y), f"{name} x{count}", fill=ACCENT_GOLD, font=font_main)
            draw.text((w - 30, y + 2), desc, fill=TEXT_MUTED, font=font_desc, anchor="rt")
            y += line_h

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_online(players: list[dict], count: int) -> bytes:
    """绘制在线玩家列表图片。"""
    line_h = 28
    padding = 20
    h = padding * 2 + 50 + max(len(players), 1) * line_h + 10
    w = 380
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(18)
    font_main = _get_font(14)

    draw.text((w // 2, 22), f"在线修士（{count}人）", fill=ACCENT_GOLD, font=font_title, anchor="mt")

    y = 55
    if not players:
        draw.text((w // 2, y), "当前无修士在线", fill=TEXT_MUTED, font=font_main, anchor="mt")
    else:
        for p in players:
            draw.text((30, y), p.get("name", ""), fill=TEXT_PRIMARY, font=font_main)
            draw.text((w - 30, y), p.get("realm_name", ""), fill=ACCENT_CYAN, font=font_main, anchor="rt")
            y += line_h

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_checkin(data: dict) -> bytes:
    """绘制每日签到结果图片。"""
    w, h = 420, 200
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(22)
    font_reward = _get_font(18)
    font_small = _get_font(13)

    # 标题
    draw.text((w // 2, 35), "每日签到", fill=ACCENT_GOLD, font=font_title, anchor="mt")

    # 奖励文字
    rewards = data.get("rewards", "")
    message = f"今日获取{rewards}"
    draw.text((w // 2, 95), message, fill=ACCENT_CYAN, font=font_reward, anchor="mm")

    # 底部提示
    draw.text((w // 2, h - 25), "修仙世界", fill=TEXT_MUTED, font=font_small, anchor="mt")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_afk_result(data: dict) -> bytes:
    """绘制挂机修炼结算结果图片。"""
    sub_ups = data.get("sub_level_ups", 0)
    h = 230 if sub_ups > 0 else 200
    w = 420
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(22)
    font_main = _get_font(18)
    font_small = _get_font(13)

    # 标题
    draw.text((w // 2, 35), "挂机修炼结算", fill=ACCENT_GOLD, font=font_title, anchor="mt")

    # 时长
    duration = data.get("duration_min", 0)
    draw.text((w // 2, 75), f"修炼时长：{duration}分钟", fill=TEXT_SECONDARY, font=font_main, anchor="mm")

    # 获得修为
    exp = data.get("exp_gained", 0)
    draw.text((w // 2, 110), f"获得修为：{exp}", fill=ACCENT_CYAN, font=font_main, anchor="mm")

    # 境界提升
    if sub_ups > 0:
        realm_name = data.get("realm_name", "")
        draw.text((w // 2, 145), f"境界提升！当前：{realm_name}", fill=ACCENT_GREEN, font=font_main, anchor="mm")

    # 底部
    draw.text((w // 2, h - 25), "修仙世界", fill=TEXT_MUTED, font=font_small, anchor="mt")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_adventure(data: dict) -> bytes:
    """绘制历练结果图片。"""
    outcome = data.get("outcome", "")
    realm_changed = data.get("realm_changed", False)
    died = data.get("died", False)

    message = str(data.get("message", ""))
    line_count = max(1, len(message.split("\n")))
    extra_h = max(0, (line_count - 2) * 22)
    h = (280 if (realm_changed or died) else 240) + extra_h
    w = 420
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(20)
    font_main = _get_font(16)
    font_desc = _get_font(13)

    # 标题：分类·场景名
    cat = data.get("category", "")
    scene = data.get("scene_name", "")
    draw.text((w // 2, 30), f"{cat}·{scene}", fill=ACCENT_GOLD, font=font_title, anchor="mt")

    # 场景描述
    desc = data.get("description", "")
    draw.text((w // 2, 58), desc, fill=TEXT_MUTED, font=font_desc, anchor="mt")

    # 难度
    diff = data.get("difficulty_label", "")
    draw.text((w // 2, 82), f"难度：{diff}", fill=TEXT_SECONDARY, font=font_desc, anchor="mt")

    # 结果
    y = 110
    if outcome == "stones":
        stones = data.get("stones_gained", 0)
        draw.text((w // 2, y), f"获得灵石：{stones}", fill=ACCENT_GOLD, font=font_main, anchor="mt")
    elif outcome == "exp":
        exp = data.get("exp_gained", 0)
        draw.text((w // 2, y), f"获得修为：{exp}", fill=ACCENT_CYAN, font=font_main, anchor="mt")
    elif outcome == "pill":
        pill = data.get("pill_name", "")
        draw.text((w // 2, y), f"获得丹药：{pill}", fill=ACCENT_GREEN, font=font_main, anchor="mt")
    elif outcome == "equip_drop":
        equip_name = data.get("equip_name", "")
        equip_tier = data.get("equip_tier", "")
        draw.text((w // 2, y), f"获得装备：{equip_tier}【{equip_name}】", fill=ACCENT_GOLD, font=font_main, anchor="mt")
    elif outcome == "reward_combo":
        combo_size = data.get("combo_size", 1)
        combo_desc = "+".join(["1"] * int(combo_size))
        draw.text((w // 2, y), f"组合掉落：{combo_desc}", fill=ACCENT_CYAN, font=font_main, anchor="mt")
        y += 24
        for line in data.get("reward_lines", []):
            draw.text((w // 2, y), line, fill=TEXT_PRIMARY, font=font_desc, anchor="mt")
            y += 20
    elif outcome == "realm_up":
        old_r = data.get("old_realm", "")
        new_r = data.get("new_realm", "")
        draw.text((w // 2, y), "奇遇顿悟，境界突破！", fill=ACCENT_CYAN, font=font_main, anchor="mt")
        draw.text((w // 2, y + 28), f"{old_r} → {new_r}", fill=ACCENT_GREEN, font=font_main, anchor="mt")
    elif outcome == "injured":
        dmg = data.get("damage", 0)
        draw.text((w // 2, y), f"苦战受伤，损失{dmg}点生命", fill=ACCENT_RED, font=font_main, anchor="mt")
    elif outcome == "injured_realm_down":
        dmg = data.get("damage", 0)
        draw.text((w // 2, y), f"九死一生！损失{dmg}点生命", fill=ACCENT_RED, font=font_main, anchor="mt")
        if realm_changed:
            old_r = data.get("old_realm", "")
            new_r = data.get("new_realm", "")
            draw.text((w // 2, y + 28), f"境界跌落：{old_r} → {new_r}", fill=ACCENT_RED, font=font_main, anchor="mt")
    elif outcome == "death":
        draw.text((w // 2, y), "不幸陨落，修为尽失……", fill=ACCENT_RED, font=font_main, anchor="mt")

    # 通用结果文案（防止新结果类型不显示）
    if message and outcome != "reward_combo":
        msg_y = h - 62
        for line in message.split("\n")[-3:]:
            draw.text((w // 2, msg_y), line, fill=TEXT_MUTED, font=font_desc, anchor="mt")
            msg_y += 18

    # 底部
    draw.text((w // 2, h - 20), "修仙世界", fill=TEXT_MUTED, font=font_desc, anchor="mt")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def render_scenes(scenes: list[dict]) -> bytes:
    """绘制历练场景列表图片。"""
    categories = {}
    for s in scenes:
        cat = s["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(s["name"])

    # 计算高度：标题 + 每个分类标题 + 每个场景行
    cat_count = len(categories)
    total_items = sum(len(v) for v in categories.values())
    h = 50 + cat_count * 35 + total_items * 22 + 30
    w = 420
    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    font_title = _get_font(20)
    font_cat = _get_font(16)
    font_item = _get_font(13)

    draw.text((w // 2, 25), "历练场景一览", fill=ACCENT_GOLD, font=font_title, anchor="mt")

    y = 55
    for cat, names in categories.items():
        draw.text((30, y), f"【{cat}】", fill=ACCENT_CYAN, font=font_cat)
        y += 28
        for name in names:
            draw.text((50, y), f"· {name}", fill=TEXT_PRIMARY, font=font_item)
            y += 22
        y += 5

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# 物品效果文本映射
_EFFECT_LABELS: dict[str, str] = {
    "heal_hp": "恢复生命",
    "exp_bonus": "获得经验",
    "breakthrough_bonus": "突破成功率",
    "attack_boost": "永久攻击力",
    "prevent_death": "免除死亡",
    "learn_heart_method": "领悟心法",
}

# 物品类型显示名
_ITEM_TYPE_LABELS: dict[str, str] = {
    "consumable": "消耗品",
    "material": "材料",
    "equipment": "装备",
    "heart_method": "心法",
    "gongfa": "功法",
}


def render_item_detail(data: dict) -> bytes:
    """绘制物品详情卡片。"""
    w = 420
    item_type = data.get("type", "item")
    name = data.get("name", "未知物品")

    font_title = _get_font(22)
    font_label = _get_font(14)
    font_main = _get_font(15)
    font_desc = _get_font(13)

    # 文本测量与换行
    _measure_img = Image.new("RGB", (1, 1), BG_COLOR)
    _measure_draw = ImageDraw.Draw(_measure_img)

    def _text_width(text: str, font) -> int:
        bbox = _measure_draw.textbbox((0, 0), str(text), font=font)
        return max(0, int(bbox[2] - bbox[0]))

    def _clip_text(text: str, font, max_width: int) -> str:
        s = str(text or "")
        if not s:
            return ""
        if _text_width(s, font) <= max_width:
            return s
        suffix = "..."
        while s and _text_width(s + suffix, font) > max_width:
            s = s[:-1]
        return (s + suffix) if s else suffix

    def _wrap_text(text: str, font, max_width: int) -> list[str]:
        raw = str(text or "").replace("\r", "")
        if not raw:
            return []
        lines: list[str] = []
        for segment in raw.split("\n"):
            if not segment:
                lines.append("")
                continue
            buf = ""
            for ch in segment:
                candidate = buf + ch
                if (not buf) or (_text_width(candidate, font) <= max_width):
                    buf = candidate
                else:
                    lines.append(buf)
                    buf = ch
            if buf:
                lines.append(buf)
        return lines

    # 根据类型构建属性行
    rows: list[tuple[str, str]] = []
    if item_type == "equipment":
        type_label = data.get("slot", "装备")
        rows.append(("品阶", data.get("tier_name", "未知")))
        if data.get("attack", 0):
            rows.append(("攻击", f"+{data['attack']}"))
        if data.get("defense", 0):
            rows.append(("防御", f"+{data['defense']}"))
        if data.get("element", "无") != "无":
            rows.append(("元素", data["element"]))
        if data.get("element_damage", 0):
            rows.append(("元素伤害", f"+{data['element_damage']}"))
    elif item_type == "heart_method":
        type_label = "心法秘籍"
        rows.append(("品质", data.get("quality_name", "未知")))
        rows.append(("对应境界", data.get("realm_name", "未知")))
        if data.get("attack_bonus", 0):
            rows.append(("攻击加成", f"+{data['attack_bonus']}"))
        if data.get("defense_bonus", 0):
            rows.append(("防御加成", f"+{data['defense_bonus']}"))
        if data.get("exp_multiplier", 0):
            rows.append(("经验倍率", f"+{data['exp_multiplier'] * 100:.0f}%"))
        if data.get("dao_yun_rate", 0):
            rows.append(("道韵获取率", f"{data['dao_yun_rate'] * 100:.0f}%"))
    elif item_type == "gongfa":
        type_label = "功法卷轴"
        rows.append(("品阶", data.get("tier_name", "未知")))
        if data.get("attack_bonus", 0):
            rows.append(("攻击加成", f"+{data['attack_bonus']}"))
        if data.get("defense_bonus", 0):
            rows.append(("防御加成", f"+{data['defense_bonus']}"))
        if data.get("hp_regen", 0):
            rows.append(("生命回复", f"+{data['hp_regen']}/次"))
        if data.get("lingqi_regen", 0):
            rows.append(("灵力回复", f"+{data['lingqi_regen']}/次"))
    else:
        type_label = _ITEM_TYPE_LABELS.get(data.get("item_type", ""), "物品")
        effect = data.get("effect", {})
        if not isinstance(effect, dict):
            effect = {}
        for eff_key, eff_val in effect.items():
            label = _EFFECT_LABELS.get(eff_key, eff_key)
            if isinstance(eff_val, bool):
                rows.append((label, "是"))
            elif isinstance(eff_val, float):
                rows.append((label, f"{eff_val * 100:+.0f}%"))
            else:
                rows.append((label, str(eff_val)))

    description = str(data.get("description", "") or "")

    # 先预计算属性行换行和总高度，避免长文本溢出
    label_x = 36
    value_right_x = w - 36
    value_left_x = 178
    label_max_width = value_left_x - label_x - 12
    value_max_width = value_right_x - value_left_x
    row_line_h = 20
    row_bottom_gap = 8

    row_blocks: list[tuple[list[str], list[str], int, int]] = []
    rows_total_h = 0
    for key, value in rows:
        key_lines = _wrap_text(str(key), font_main, label_max_width) or [""]
        value_lines = _wrap_text(str(value), font_main, value_max_width) or [""]
        line_count = max(len(key_lines), len(value_lines), 1)
        row_h = line_count * row_line_h + row_bottom_gap
        row_blocks.append((key_lines, value_lines, line_count, row_h))
        rows_total_h += row_h

    desc_lines = _wrap_text(description, font_desc, w - 72) if description else []
    desc_line_h = 18
    desc_total_h = len(desc_lines) * desc_line_h

    # 顶部(标题+类型+分隔) + 属性区 + 描述区 + 底部留白
    h = 28 + 32 + 28 + 12 + rows_total_h + 22
    if desc_lines:
        h += 4 + 10 + desc_total_h + 8
    h = max(h, 180)

    img = Image.new("RGB", (w, h), BG_COLOR)
    draw = ImageDraw.Draw(img)
    _draw_rounded_rect(draw, (10, 10, w - 10, h - 10), CARD_BG, radius=12)

    y = 28
    # 标题
    draw.text((w // 2, y), _clip_text(name, font_title, w - 64), fill=ACCENT_GOLD, font=font_title, anchor="mt")
    y += 32
    # 类型标签
    draw.text((w // 2, y), _clip_text(f"[ {type_label} ]", font_label, w - 70), fill=ACCENT_CYAN, font=font_label, anchor="mt")
    y += 28

    # 分隔线
    draw.line((24, y, w - 24, y), fill=BORDER_COLOR, width=1)
    y += 12

    # 属性行
    for key_lines, value_lines, line_count, row_h in row_blocks:
        for idx in range(line_count):
            line_y = y + idx * row_line_h
            if idx < len(key_lines):
                draw.text((label_x, line_y), key_lines[idx], fill=TEXT_SECONDARY, font=font_main)
            if idx < len(value_lines):
                draw.text((value_right_x, line_y), value_lines[idx], fill=TEXT_PRIMARY, font=font_main, anchor="rt")
        y += row_h

    # 描述
    if desc_lines:
        y += 2
        draw.line((24, y, w - 24, y), fill=BORDER_COLOR, width=1)
        y += 10
        for line in desc_lines:
            draw.text((36, y), line, fill=TEXT_MUTED, font=font_desc)
            y += desc_line_h

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ── 品阶颜色映射 ──────────────────────────────────────
_TIER_COLORS = {
    0: TEXT_SECONDARY,   # 凡品
    1: ACCENT_GREEN,     # 灵品
    2: ACCENT_CYAN,      # 玄品
    3: ACCENT_PURPLE,    # 地品
    4: ACCENT_GOLD,      # 天品
    5: ACCENT_RED,       # 仙品
}


def render_market(
    listings: list[dict],
    page: int = 1,
    total_pages: int = 1,
) -> bytes:
    """绘制坊市商品列表（矩形网格 2 列）。"""
    import time as _time

    img_w = 580
    cols = 2
    card_w = 270
    card_h = 100
    gap_x = 16
    gap_y = 12
    margin_x = (img_w - cols * card_w - (cols - 1) * gap_x) // 2
    margin_top = 56
    margin_bottom = 40

    font_title = _get_font(20)
    font_name = _get_font(14)
    font_price = _get_font(13)
    font_small = _get_font(11)

    if not listings:
        h = 180
        img = Image.new("RGB", (img_w, h), BG_COLOR)
        draw = ImageDraw.Draw(img)
        _draw_rounded_rect(draw, (10, 10, img_w - 10, h - 10), CARD_BG, radius=12)
        draw.text(
            (img_w // 2, 40), "坊 市", fill=ACCENT_GOLD, font=font_title, anchor="mt",
        )
        draw.text(
            (img_w // 2, 90), "坊市暂无商品", fill=TEXT_MUTED, font=font_name, anchor="mt",
        )
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    rows_count = (len(listings) + cols - 1) // cols
    grid_h = rows_count * card_h + (rows_count - 1) * gap_y
    img_h = margin_top + grid_h + margin_bottom

    img = Image.new("RGB", (img_w, img_h), BG_COLOR)
    draw = ImageDraw.Draw(img)
    _draw_rounded_rect(draw, (10, 10, img_w - 10, img_h - 10), CARD_BG, radius=12)

    # 标题
    draw.text(
        (img_w // 2, 24), "坊 市", fill=ACCENT_GOLD, font=font_title, anchor="mt",
    )

    now = _time.time()

    for idx, listing in enumerate(listings):
        row_i = idx // cols
        col_i = idx % cols
        x = margin_x + col_i * (card_w + gap_x)
        y = margin_top + row_i * (card_h + gap_y)

        # 卡片背景
        _draw_rounded_rect(draw, (x, y, x + card_w, y + card_h), (26, 26, 54), radius=8)

        # 物品名 - 带品阶颜色
        item_name = listing.get("item_name", "?")
        # 截断长名称
        if len(item_name) > 8:
            item_name = item_name[:7] + "…"
        name_color = ACCENT_GOLD
        draw.text((x + 10, y + 8), item_name, fill=name_color, font=font_name)

        # 短编号
        short_id = listing.get("listing_id", "")[:6]
        draw.text(
            (x + card_w - 10, y + 10), f"#{short_id}",
            fill=TEXT_MUTED, font=font_small, anchor="rt",
        )

        # 数量和总价
        qty = listing.get("quantity", 0)
        total_price = listing.get("total_price", 0)
        draw.text(
            (x + 10, y + 32), f"x{qty}",
            fill=TEXT_SECONDARY, font=font_price,
        )
        draw.text(
            (x + 70, y + 32), f"{total_price} 灵石",
            fill=ACCENT_GOLD, font=font_price,
        )

        # 单价
        unit_price = listing.get("unit_price", 0)
        draw.text(
            (x + card_w - 10, y + 32), f"单价{unit_price}",
            fill=TEXT_MUTED, font=font_small, anchor="rt",
        )

        # 卖家名（截断）
        seller_name = listing.get("seller_name", "未知")
        if len(seller_name) > 6:
            seller_name = seller_name[:5] + "…"
        draw.text(
            (x + 10, y + 56), f"卖家: {seller_name}",
            fill=TEXT_MUTED, font=font_small,
        )

        # 剩余时间
        expires_at = listing.get("expires_at", 0)
        remain = max(0, expires_at - now)
        if remain >= 3600:
            time_str = f"{int(remain // 3600)}h{int((remain % 3600) // 60)}m"
        elif remain >= 60:
            time_str = f"{int(remain // 60)}m"
        else:
            time_str = "即将过期"
        draw.text(
            (x + card_w - 10, y + 56), time_str,
            fill=TEXT_MUTED, font=font_small, anchor="rt",
        )

        # 底部分隔线
        draw.line(
            (x + 8, y + card_h - 18, x + card_w - 8, y + card_h - 18),
            fill=BORDER_COLOR, width=1,
        )

        # 购买提示
        draw.text(
            (x + card_w // 2, y + card_h - 8),
            f"修仙 购买 {short_id}",
            fill=ACCENT_CYAN, font=font_small, anchor="mt",
        )

    # 底部分页
    page_text = f"第 {page}/{total_pages} 页"
    draw.text(
        (img_w // 2, img_h - 20), page_text,
        fill=TEXT_SECONDARY, font=font_small, anchor="mt",
    )

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()
