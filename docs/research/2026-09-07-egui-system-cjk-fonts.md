# egui 系统 CJK 字体调研

日期：2026-09-07。范围：原生 macOS、Windows、Linux；wasm 可暂缓。本次仅调研，未修改应用代码，未实测安装包大小或跨平台渲染。

## 建议

使用 **fontdb 0.24 发现系统字体 + egui FontData 加载字体字节及 face index**。保留 egui 默认字体，将选中的 CJK 字体追加到 Proportional 和 Monospace 的 fallback 列表。这样新增字体资源的打包体积为零，新增 Rust 代码仍有体积成本；准确增量需同配置 release 构建比较。[1][2][3]

本项目 desktop/web 均使用 eframe 0.36.1，启用 default_fonts，目前没有自定义 set_fonts。建议仅在 `apps/k10s-desktop/src/main.rs` 的 `run_native` 创建回调中通过 `cc.egui_ctx` 安装字体；系统发现代码放在 desktop crate，避免进入共享 UI 和 wasm。

## 方案比较

| 方案 | 适配情况 | 判断 |
| --- | --- | --- |
| fontdb 0.24 | 系统目录发现、字体族查询、TTC index；无需链接系统字体库 | 首选，应用自行定义 CJK 候选顺序 |
| egui-system-fonts 0.36.0 | egui ^0.36 可匹配本项目；底层 system-fonts 0.1.2 丢失 TTC index | 暂不直接使用，除非先修复 index 传递 |
| font-kit 0.14.3 | Core Text / DirectWrite / Fontconfig 系统 API；还有光栅化等能力 | 需要更完整的系统字体发现时考虑，当前功能和平台依赖偏多 |
| 手写固定路径加载 | 无字体发现库依赖 | 代码可能最少，但路径、用户字体、TTC face 选择及 Linux 发行版适配都需自行维护 |
| 内嵌 Noto CJK / 下载字体 | 不依赖系统安装 | 不符合本次优先加载系统字体、尽量减小体积的方向 |

以上依赖/API判断见 [2]–[6]；没有测量各方案最终二进制增量。

## 关键实现细节

1. 调用 `Database::load_system_fonts()`，按候选字体族查询正常字重。fontdb 扫描预定义目录，并不调用各操作系统完整字体选择 API；特殊目录可通过应用配置补充。[2]
2. 使用 `with_face_data(id, |bytes, index| ...)`。通过 `FontData::from_owned(bytes.to_vec())` 创建数据后，**必须设置 `data.index = index`**。TTC 集合包含多个 face，固定为 0 可能选错字重、字体族或地区字形。[1][2]
3. 从 `FontDefinitions::default()` 开始，对两种字体 family 使用 `push` 追加 fallback，再调用一次 `ctx.set_fonts(defs)`。默认 Latin/符号字体仍优先，egui 对缺字按 fallback 顺序查询。[1]
4. CJK 并非一种字体必定全覆盖。建议选择主要汉字地区字体，并按需要补充假名、韩文覆盖。仅找到中文字体不能宣称完整 CJK 支持。fontdb 查询字体族不等于验证字符覆盖。[2]
5. 字体缺失、不可读或解析失败时继续尝试候选；全部失败则保留默认字体并记录诊断。Linux 精简环境可能没有 CJK 字库，届时提示安装 Noto CJK，或允许配置用户字体文件。
6. 初始化时发现和加载一次，不在每帧扫描。同一 face 在两个 egui family 中复用一个注册条目。避免加载全部系统字体；同一大型 TTC 的多个 face 通过 `from_owned` 分别加载可能重复持有整份文件。包体不增加字体资源，不代表运行时内存零成本。[1][2]
7. 按应用或用户语言调整汉字字体优先级：egui 的逐字符 fallback 不是地区字形自动选择器。把比例 CJK 字体加到 Monospace 可解决缺字，但不保证终端严格双列宽度，需要另行检查日志/终端布局。[1]

建议候选（以实际发现为准，不假定所有机器都有）：

| 平台 | 中文 | 日文 | 韩文 |
| --- | --- | --- | --- |
| macOS | PingFang SC / TC / HK | Hiragino Sans | Apple SD Gothic Neo |
| Windows | Microsoft YaHei / Microsoft JhengHei | Yu Gothic / Meiryo | Malgun Gothic |
| Linux | Noto Sans CJK SC / TC / HK | Noto Sans CJK JP | Noto Sans CJK KR |

Apple/Microsoft 字体清单见 [7][8]；Linux 候选来自 Noto CJK 项目 [9]。系统版本、语言组件和安装状态会影响可用性。

## 依赖与体积

fontdb 0.24 默认 features 为 `std`, `fs`, `memmap`, `fontconfig`，基础依赖为 log、slotmap、tinyvec；memmap2 和 fontconfig-parser 可选。0.24 的依赖清单已经没有旧版的 ttf-parser，不能根据旧 README 推断最新版依赖。[3]

建议先用默认 features。`fontconfig` 是解析配置文件，不是链接 libfontconfig；官方特别指出 NixOS 需要它。若进一步追求体积，可以对比 `default-features = false, features = ["fs", "fontconfig"]`，但去掉 mmap 可能增加字体扫描的读取/分配开销，不应在未测量时宣称更优。[2][3]

`egui-system-fonts` 的 TTC 问题来自两层源码：system-fonts 的 FoundFont 只传路径/字节，未传 face index；egui 包装器直接使用默认 index 为 0 的 `FontData::from_owned`。其 wasm 分支通过 jsDelivr 下载 Noto 字体，不是读取系统字体。[4][5]

## wasm

第一阶段跳过。浏览器 `queryLocalFonts()` 支持有限，需要安全上下文、用户授权，且受 Permissions Policy 等限制。egui canvas 不会自动继承 CSS 的系统字体 fallback。以后如需 web CJK，优先单独设计异步获取字体资源并注入 egui 的流程；这避免字体嵌入 wasm，但仍有下载与缓存成本。[1][10]

## 后续验证

实际接入时检查：非零 TTC index；简中、繁中、假名、韩文在两种 family 中的显示；没有 CJK 字体时能正常启动；默认 Latin/符号显示；启动耗时、峰值内存及同配置 release 文件大小。仅判断 glyph width 大于零不能证明字符存在，因为替代字符也可能有宽度。[1]

## 来源

1. [egui/epaint 0.36.1 字体定义及 fallback 源码](https://github.com/emilk/egui/blob/0.36.1/crates/epaint/src/text/fonts.rs)，[字体解析源码](https://github.com/emilk/egui/blob/0.36.1/crates/epaint/src/text/font.rs)
2. [fontdb 0.24 API](https://docs.rs/fontdb/0.24.0/fontdb/struct.Database.html)，[系统发现源码](https://github.com/RazrFalcon/fontdb/blob/master/src/lib.rs)
3. [fontdb Cargo.toml](https://github.com/RazrFalcon/fontdb/blob/master/Cargo.toml)，[0.24.0 发布依赖](https://crates.io/api/v1/crates/fontdb/0.24.0/dependencies)
4. [egui-system-fonts 源码](https://github.com/yijehyung/egui-system-fonts/blob/main/egui-system-fonts/src/lib.rs)，[Cargo.toml](https://github.com/yijehyung/egui-system-fonts/blob/main/egui-system-fonts/Cargo.toml)
5. [system-fonts 源码](https://github.com/yijehyung/system-fonts/blob/main/src/lib.rs)
6. [font-kit README](https://github.com/servo/font-kit/blob/master/README.md)，[Cargo.toml](https://github.com/servo/font-kit/blob/master/Cargo.toml)
7. [Apple macOS Sonoma 字体清单](https://support.apple.com/en-us/120414)
8. [Microsoft Windows 11 字体清单](https://learn.microsoft.com/en-us/typography/fonts/windows_11_font_list)
9. [Noto CJK 官方项目](https://github.com/notofonts/noto-cjk)
10. [MDN queryLocalFonts](https://developer.mozilla.org/en-US/docs/Web/API/Window/queryLocalFonts)
