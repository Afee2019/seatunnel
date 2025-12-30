# ============================================================================
# Apache SeaTunnel - Makefile
# ============================================================================
# 项目组件:
#   - Java Zeta Engine    (端口 8216)  - 核心引擎
#   - Go Gateway          (端口 8215)  - REST API 网关
#   - Web UI              (端口 8214)  - 前端界面
# ============================================================================

.PHONY: all help status start stop restart
.PHONY: start-java start-go start-ui
.PHONY: stop-java stop-go stop-ui
.PHONY: build build-java build-go
.PHONY: logs-java logs-go logs-ui
.PHONY: clean

# ============================================================================
# 变量定义
# ============================================================================

# 端口配置
PORT_UI := 8214
PORT_GO := 8215
PORT_JAVA := 8216

# 目录配置
ROOT_DIR := $(shell pwd)
GO_DIR := $(ROOT_DIR)/seatunnel-go
UI_DIR := $(ROOT_DIR)/seatunnel-engine/seatunnel-engine-ui
DIST_DIR := $(ROOT_DIR)/seatunnel-dist/target/apache-seatunnel-2.3.13-SNAPSHOT

# Java 配置 (使用 Java 21)
JAVA_HOME := /Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home

# 日志配置
LOG_DIR := $(ROOT_DIR)/logs
LOG_JAVA := $(LOG_DIR)/java-engine.log
LOG_GO := $(LOG_DIR)/go-gateway.log
LOG_UI := $(LOG_DIR)/web-ui.log

# ============================================================================
# 帮助信息
# ============================================================================

all: help

help: ## 显示帮助信息
	@echo "Apache SeaTunnel Make 命令"
	@echo "=================================================="
	@echo ""
	@echo "端口分配:"
	@echo "  8214  Web UI (Vue.js + Vite)"
	@echo "  8215  Go Gateway (REST API)"
	@echo "  8216  Java Zeta Engine"
	@echo ""
	@echo "🚀 快捷命令:"
	@grep -E '^(help|status|start|stop|restart):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "🔧 单独服务管理:"
	@grep -E '^(start-|stop-|logs-)[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[33m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "🏗️ 构建:"
	@grep -E '^(build|build-|clean)[a-zA-Z_-]*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[32m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# 服务状态
# ============================================================================

status: ## 查看所有服务状态
	@echo "=== SeaTunnel 服务状态 ==="
	@echo ""
	@echo "Java Zeta Engine (端口 $(PORT_JAVA)):"
	@echo "----------------------------------------"
	@PID=$$(lsof -ti:$(PORT_JAVA) 2>/dev/null); \
	if [ -n "$$PID" ]; then \
		echo "  状态: \033[32m✓ 运行中\033[0m"; \
		echo "  PID: $$PID"; \
		echo "  运行时间: $$(ps -o etime= -p $$PID 2>/dev/null | xargs)"; \
	else \
		echo "  状态: \033[31m✗ 未运行\033[0m"; \
	fi
	@echo ""
	@echo "Go Gateway (端口 $(PORT_GO)):"
	@echo "----------------------------------------"
	@PID=$$(lsof -ti:$(PORT_GO) 2>/dev/null); \
	if [ -n "$$PID" ]; then \
		echo "  状态: \033[32m✓ 运行中\033[0m"; \
		echo "  PID: $$PID"; \
		echo "  运行时间: $$(ps -o etime= -p $$PID 2>/dev/null | xargs)"; \
	else \
		echo "  状态: \033[31m✗ 未运行\033[0m"; \
	fi
	@echo ""
	@echo "Web UI (端口 $(PORT_UI)):"
	@echo "----------------------------------------"
	@PID=$$(lsof -ti:$(PORT_UI) 2>/dev/null); \
	if [ -n "$$PID" ]; then \
		echo "  状态: \033[32m✓ 运行中\033[0m"; \
		echo "  PID: $$PID"; \
		echo "  运行时间: $$(ps -o etime= -p $$PID 2>/dev/null | xargs)"; \
	else \
		echo "  状态: \033[31m✗ 未运行\033[0m"; \
	fi
	@echo "----------------------------------------"

# ============================================================================
# 启动服务 (按依赖顺序: Java → Go → UI)
# ============================================================================

start: start-java start-go start-ui ## 启动所有服务 (按依赖顺序)
	@echo ""
	@echo "=== 所有服务已启动 ==="
	@make status

start-java: ## 启动 Java Zeta Engine (:8216)
	@if lsof -ti:$(PORT_JAVA) >/dev/null 2>&1; then \
		echo "Java Zeta Engine 已在运行 (端口 $(PORT_JAVA))"; \
	else \
		echo "正在启动 Java Zeta Engine (端口 $(PORT_JAVA))..."; \
		echo "  使用 Java: $(JAVA_HOME)"; \
		mkdir -p $(LOG_DIR); \
		if [ -d "$(DIST_DIR)" ]; then \
			cd $(DIST_DIR) && JAVA_HOME=$(JAVA_HOME) PATH=$(JAVA_HOME)/bin:$$PATH nohup ./bin/seatunnel-cluster.sh > $(LOG_JAVA) 2>&1 & \
			sleep 5; \
			if lsof -ti:$(PORT_JAVA) >/dev/null 2>&1; then \
				echo "  \033[32m✓ Java Zeta Engine 已启动\033[0m"; \
			else \
				echo "  \033[31m✗ 启动失败，请检查日志: $(LOG_JAVA)\033[0m"; \
			fi; \
		else \
			echo "  \033[31m✗ 未找到发布包，请先执行 make build-java\033[0m"; \
		fi; \
	fi

start-go: ## 启动 Go Gateway (:8215)
	@if lsof -ti:$(PORT_GO) >/dev/null 2>&1; then \
		echo "Go Gateway 已在运行 (端口 $(PORT_GO))"; \
	else \
		echo "正在启动 Go Gateway (端口 $(PORT_GO))..."; \
		mkdir -p $(LOG_DIR); \
		if [ -f "$(GO_DIR)/bin/seatunnel-gateway" ]; then \
			cd $(GO_DIR) && nohup ./bin/seatunnel-gateway > $(LOG_GO) 2>&1 & \
			sleep 2; \
			if lsof -ti:$(PORT_GO) >/dev/null 2>&1; then \
				echo "  \033[32m✓ Go Gateway 已启动\033[0m"; \
			else \
				echo "  \033[31m✗ 启动失败，请检查日志: $(LOG_GO)\033[0m"; \
			fi; \
		else \
			echo "  \033[31m✗ 未找到二进制文件，请先执行 make build-go\033[0m"; \
		fi; \
	fi

start-ui: ## 启动 Web UI (:8214)
	@if lsof -ti:$(PORT_UI) >/dev/null 2>&1; then \
		echo "Web UI 已在运行 (端口 $(PORT_UI))"; \
	else \
		echo "正在启动 Web UI (端口 $(PORT_UI))..."; \
		mkdir -p $(LOG_DIR); \
		if [ -d "$(UI_DIR)" ]; then \
			cd $(UI_DIR) && nohup npm run dev > $(LOG_UI) 2>&1 & \
			sleep 3; \
			if lsof -ti:$(PORT_UI) >/dev/null 2>&1; then \
				echo "  \033[32m✓ Web UI 已启动\033[0m"; \
				echo "  访问地址: http://localhost:$(PORT_UI)"; \
			else \
				echo "  \033[31m✗ 启动失败，请检查日志: $(LOG_UI)\033[0m"; \
			fi; \
		else \
			echo "  \033[31m✗ 未找到 UI 目录\033[0m"; \
		fi; \
	fi

# ============================================================================
# 停止服务 (按依赖顺序: UI → Go → Java)
# ============================================================================

stop: stop-ui stop-go stop-java ## 停止所有服务 (按依赖顺序)
	@echo ""
	@echo "=== 所有服务已停止 ==="

stop-ui: ## 停止 Web UI (:8214)
	@echo "正在停止 Web UI (端口 $(PORT_UI))..."
	@PID=$$(lsof -ti:$(PORT_UI) 2>/dev/null); \
	if [ -n "$$PID" ]; then \
		kill $$PID 2>/dev/null || true; \
		sleep 1; \
		if lsof -ti:$(PORT_UI) >/dev/null 2>&1; then \
			kill -9 $$PID 2>/dev/null || true; \
		fi; \
		echo "  \033[32m✓ Web UI 已停止\033[0m"; \
	else \
		echo "  Web UI 未运行"; \
	fi

stop-go: ## 停止 Go Gateway (:8215)
	@echo "正在停止 Go Gateway (端口 $(PORT_GO))..."
	@PID=$$(lsof -ti:$(PORT_GO) 2>/dev/null); \
	if [ -n "$$PID" ]; then \
		kill $$PID 2>/dev/null || true; \
		sleep 1; \
		if lsof -ti:$(PORT_GO) >/dev/null 2>&1; then \
			kill -9 $$PID 2>/dev/null || true; \
		fi; \
		echo "  \033[32m✓ Go Gateway 已停止\033[0m"; \
	else \
		echo "  Go Gateway 未运行"; \
	fi

stop-java: ## 停止 Java Zeta Engine (:8216)
	@echo "正在停止 Java Zeta Engine (端口 $(PORT_JAVA))..."
	@PID=$$(lsof -ti:$(PORT_JAVA) 2>/dev/null); \
	if [ -n "$$PID" ]; then \
		kill $$PID 2>/dev/null || true; \
		sleep 2; \
		if lsof -ti:$(PORT_JAVA) >/dev/null 2>&1; then \
			kill -9 $$PID 2>/dev/null || true; \
		fi; \
		echo "  \033[32m✓ Java Zeta Engine 已停止\033[0m"; \
	else \
		echo "  Java Zeta Engine 未运行"; \
	fi

# ============================================================================
# 重启服务
# ============================================================================

restart: stop start ## 重启所有服务

# ============================================================================
# 构建
# ============================================================================

build: build-java build-go ## 构建所有组件

build-java: ## 构建 Java 项目
	@echo "正在构建 Java 项目..."
	./mvnw clean package -pl seatunnel-dist -am -Dmaven.test.skip=true -T 1C
	@echo "  \033[32m✓ Java 构建完成\033[0m"

build-go: ## 构建 Go Gateway
	@echo "正在构建 Go Gateway..."
	cd $(GO_DIR) && go build -o bin/seatunnel-gateway ./cmd/gateway
	cd $(GO_DIR) && go build -o bin/seatunnel ./cmd/seatunnel
	@echo "  \033[32m✓ Go 构建完成\033[0m"

# ============================================================================
# 日志查看
# ============================================================================

logs-java: ## 查看 Java 引擎日志
	@if [ -f $(LOG_JAVA) ]; then \
		tail -f $(LOG_JAVA); \
	else \
		echo "日志文件不存在: $(LOG_JAVA)"; \
	fi

logs-go: ## 查看 Go Gateway 日志
	@if [ -f $(LOG_GO) ]; then \
		tail -f $(LOG_GO); \
	else \
		echo "日志文件不存在: $(LOG_GO)"; \
	fi

logs-ui: ## 查看 Web UI 日志
	@if [ -f $(LOG_UI) ]; then \
		tail -f $(LOG_UI); \
	else \
		echo "日志文件不存在: $(LOG_UI)"; \
	fi

# ============================================================================
# 清理
# ============================================================================

clean: ## 清理构建产物和日志
	@echo "正在清理..."
	@rm -rf $(LOG_DIR)
	@rm -rf $(GO_DIR)/bin
	@echo "  \033[32m✓ 清理完成\033[0m"
