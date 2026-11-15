# Yi Android

Yi 的 Android 客户端,由一个 WebView 页面和一个可选的常驻提醒服务组成。

- WebView 加载 Yi 前端,通过 SSE 展示**所有类型**的事件。
- 前台服务每 20 秒轮询 Yi 服务端口,按事件类型/关键词过滤,并发送 Android 通知。
- 前台服务持有 `PARTIAL_WAKE_LOCK`,在应用退到后台后继续工作,重启后由
  `BootReceiver` 恢复。
- 后台提醒**默认关闭**,在菜单里配置并勾选开关后启动。

## 功能

- 默认前端地址:`http://120.53.234.103:9093/`
- 默认服务地址:`http://120.53.234.103:9092/`
- 右上角菜单:
  - **刷新**
  - **服务器**:修改 Yi 前端地址
  - **后台提醒**:配置服务端口、token、类型、关键词和常驻开关
  - **浏览器打开**
- WebView 支持 JavaScript、DOM Storage、Cookie、SSE
- 服务端开启认证后,WebView 内直接使用 Yi 登录页;后台服务使用配置的 Bearer token
- 后台提醒支持:
  - 事件类型过滤(留空 = 全部类型)
  - 内容关键词过滤(留空 = 不过滤,大小写不敏感)
  - 类型和关键词同时填写时需要同时匹配(AND)
  - 新事件通知;首次启用从当前最新事件开始,不补发历史
  - 常驻前台通知,可从通知里直接停止监听
- 返回键优先在 WebView 历史中后退
- 网络错误显示可重试的本地错误页
- 明文 HTTP 已允许;公网建议使用 HTTPS 反向代理

## 后台提醒

点右上角菜单 → **后台提醒**,填写:

```text
服务端口地址: http://服务器:9092/
认证 token:   服务端的 auth-token,留空表示未开启认证
主题类型:     mail.received,note,...
内容关键词:   合同,发票,...
[✓] 后台常驻,并在匹配时通知
```

保存后:

- 启动前台服务,通知栏显示“Yi 后台监听中”
- 每 20 秒 GET `/events?limit=200`
- 使用 `Authorization: Bearer <token>` 认证
- 对新增事件做类型/关键词过滤,匹配时发通知
- 通知点击打开 Yi;前台通知带“停止”操作
- Android 13+ 会请求通知权限;如果拒绝,服务仍运行,但提醒可能不可见
- 服务会持有 partial wake lock,会消耗电量;部分厂商的省电策略仍可能杀掉它,
  需要时把 Yi 加入电池优化白名单
- 这是轮询式后台服务,不是系统推送;Doze/网络切换时可能有延迟

## 文件

```text
app/
├── AndroidManifest.xml
├── src/com/gresces/yi/MainActivity.java
├── src/com/gresces/yi/YiWatchService.java
├── src/com/gresces/yi/BootReceiver.java
├── res/drawable/ic_launcher.xml
├── res/drawable/ic_notification.xml
├── build.sh
└── README.md
```

## 构建

需要 Android SDK Platform 34 和 Build Tools(建议 35.0.0;34.0.0 在 JDK 25 下
D8 会崩溃),以及 JDK 11+(推荐 17/21):

```sh
export ANDROID_SDK_ROOT=$HOME/android-sdk
./app/build.sh
```

产物:`app/build/yi-debug.apk`。脚本带 `aapt2 --debug-mode`,这是可调试的测试
APK;发布版本应移除该参数并换用正式签名。

也可以用 `BUILD_TOOLS=/path/to/build-tools/35.0.0` 指定版本。

## 安装和启动

```sh
adb install -r app/build/yi-debug.apk
adb shell am start -n com.gresces.yi/.MainActivity
```

或者:

```sh
./app/build.sh install
```

## 修改服务器地址

在应用内点右上角菜单 → **服务器**,填写 Yi 前端地址,例如:

```text
http://192.168.1.10:9093/
```

也可以直接通过 adb 传入一次地址(会保存为默认地址):

```sh
adb shell am start -n com.gresces.yi/.MainActivity \
  --es yi_url http://192.168.1.10:9093/
```

如果要通过 USB 调试连接电脑上的 Yi:

```sh
# 电脑上运行 Yi 前端在 9093,然后:
adb reverse tcp:9093 tcp:9093

adb shell am start -n com.gresces.yi/.MainActivity \
  --es yi_url http://127.0.0.1:9093/
```

## 所有事件类型

应用不自己注册 Yi consumer,而是加载 Yi 前端。前端通过:

```text
GET /api/stream  (SSE snapshot)
```

获取全量事件快照,快照同时包含事件、类型计数、消费者和失败记录,因此默认就能
看到所有事件类型。前端的事件页如果手动选择了某个类型筛选,清除筛选即可回到全部。
