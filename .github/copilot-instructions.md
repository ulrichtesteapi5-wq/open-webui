# Instruções para IA (Open WebUI)

## Uso obrigatório de ferramentas
- SEMPRE use TODAS as ferramentas disponíveis em cada tarefa e SEMPRE use MCP Graph-It-Live.
- Use as ferramentas na melhor ordem possível: descubra contexto, valide dependências/impacto, só então edite/execute.

## Ferramentas disponíveis e melhor uso
### Exploração e contexto
- `list_dir`: mapear estrutura antes de assumir caminhos.
- `file_search`: localizar arquivos por glob; use o padrão completo pedido pelo usuário.
- `grep_search`: buscar strings/regex em arquivos; use `includePattern` para limitar escopo.
- `semantic_search`: quando não souber termos exatos; ótimo para encontrar “onde fica isso?”.
- `read_file`: ler trechos grandes (evite muitas leituras pequenas).

### Edição de arquivos
- `apply_patch`: única forma de editar arquivos texto; faça o menor diff possível.
- `create_file`: apenas para arquivos novos.
- `create_directory`: quando precisar criar pastas novas.

### Jupyter/Notebooks
- `copilot_getNotebookSummary`: listar células e ids.
- `edit_notebook_file`: inserir/editar/remover células.
- `run_notebook_cell`: executar células de código.
- `read_notebook_cell_output`: ler saída existente.

### Execução e tarefas
- `run_in_terminal`: executar comandos; **um por vez** e aguarde retorno.
- `await_terminal`: aguardar processos em background.
- `get_terminal_output`: verificar logs de processo em background.
- `kill_terminal`: encerrar processos longos.
- `create_and_run_task`: criar task quando usuário pedir build/run e não houver tasks.json.
- `get_task_output`: inspecionar saída de tasks.

### Qualidade e erros
- `get_errors`: ver erros de lint/compilação após mudanças.
- `test_failure`: quando o usuário citar falhas de teste.

### Dependências e impacto (MCP Graph-It-Live)
- `mcp_graph-it-live_graphitlive_set_workspace`: **sempre** configurar workspace antes das demais.
- `mcp_graph-it-live_graphitlive_get_index_status`: checar status do índice.
- `mcp_graph-it-live_graphitlive_analyze_dependencies`: ver imports/resoluções reais.
- `mcp_graph-it-live_graphitlive_parse_imports`: ver imports “crus”.
- `mcp_graph-it-live_graphitlive_resolve_module_path`: resolver um specifier para caminho real.
- `mcp_graph-it-live_graphitlive_crawl_dependency_graph`: entender arquitetura/árvore.
- `mcp_graph-it-live_graphitlive_expand_node`: expandir grafo incrementalmente.
- `mcp_graph-it-live_graphitlive_find_referencing_files`: descobrir quem importa um arquivo.
- `mcp_graph-it-live_graphitlive_get_symbol_graph`: dependências em nível de símbolo.
- `mcp_graph-it-live_graphitlive_get_symbol_dependents`: quem usa um símbolo.
- `mcp_graph-it-live_graphitlive_get_symbol_callers`: quem chama um símbolo.
- `mcp_graph-it-live_graphitlive_trace_function_execution`: rastrear call chain.
- `mcp_graph-it-live_graphitlive_get_impact_analysis`: blast radius antes de refatorar.
- `mcp_graph-it-live_graphitlive_verify_dependency_usage`: import usado de fato.
- `mcp_graph-it-live_graphitlive_find_unused_symbols`: detectar exports mortos.
- `mcp_graph-it-live_graphitlive_invalidate_files`: invalidar cache após alterações.
- `mcp_graph-it-live_graphitlive_rebuild_index`: reconstruir índice quando necessário.
- `mcp_graph-it-live_graphitlive_analyze_breaking_changes`: detectar mudanças breaking em assinaturas.

### Python: ambiente e execução
- `configure_python_environment`: **sempre** antes de qualquer ação Python.
- `get_python_environment_details`: ver versão/pacotes.
- `get_python_executable_details`: obter comando Python correto.
- `install_python_packages`: instalar dependências.
- `activate_python_code_validation_and_execution`: habilitar validação/execução.
- `activate_import_analysis_and_dependency_management`: habilitar análise de imports.
- `activate_python_environment_management`: habilitar gestão de ambientes.
- `activate_workspace_structure_and_file_management`: habilitar listagem de arquivos Python.

### Python: qualidade e refatoração (Pylance)
- `mcp_pylance_mcp_s_pylanceDocuments`: consultar docs do Pylance.
- `mcp_pylance_mcp_s_pylanceInvokeRefactoring`: aplicar refactors (imports, type hints, fixAll).

### Qualidade Python (Skill)
- Gates obrigatórios: `black`, `ruff`, `mypy`, `semgrep`, `vulture`, `bandit`, `pip-audit`, `codespell`.
- Script oficial: `run_quality.bat` (ordem: black → ruff --fix → mypy → semgrep → vulture → bandit → pip-audit → codespell).
- Pré-commit obrigatório: `pre-commit install` e `pre-commit run --all-files`.
- Ferramentas opcionais (não bloqueiam): `pylint`, `pyright`, `safety`, `radon`, `sourcery-cli`, `pyupgrade`.

### Web/Docs/Extensões
- `fetch_webpage`: buscar conteúdo de URLs.
- `open_simple_browser`: pré-visualizar sites.
- `get_vscode_api`: consultar API do VS Code (extensions).
- `vscode_searchExtensions_internal`: procurar extensões.
- `install_extension`: instalar extensão (em setup).

### Git e navegação
- `get_changed_files`: inspecionar diffs.
- `list_code_usages`: localizar usos de símbolos.

### Multi-tool e agentes
- `multi_tool_use.parallel`: rodar ferramentas em paralelo quando possível (exceto `semantic_search`).
- `runSubagent`: delegar pesquisa/varredura complexa.
- `switch_agent` (Plan): usar para exploração/planejamento antes de grandes mudanças.

### Outras utilidades
- `renderMermaidDiagram`: renderizar diagramas.
- `mcp_io_github_ups_resolve-library-id` + `mcp_io_github_ups_get-library-docs`: docs atualizadas de libs.
- `container-tools_get-config`: obter comandos corretos de container/compose.
- `skillNinja_*`: gerenciar skills (quando aplicável).

## Visão geral
- Monorepo com frontend SvelteKit (Svelte 5 + Vite) em [src](src) e backend Python/FastAPI em [backend/open_webui](backend/open_webui).
- O backend é o ponto de verdade: configurações centralizadas em [backend/open_webui/config.py](backend/open_webui/config.py) e orquestração do app/routers em [backend/open_webui/main.py](backend/open_webui/main.py).

## Arquitetura e fluxo
- API HTTP: FastAPI app em `open_webui.main:app`, com routers separados por domínio em [backend/open_webui/routers](backend/open_webui/routers) (ex.: `auths`, `chats`, `models`, `retrieval`).
- Socket/tempo real: camada dedicada em [backend/open_webui/socket/main.py](backend/open_webui/socket/main.py), importada no app principal.
- Persistência: modelos ORM em [backend/open_webui/models](backend/open_webui/models) e sessão/engine em [backend/open_webui/internal/db](backend/open_webui/internal/db).
- UI: rotas e layouts SvelteKit em [src/routes](src/routes); componentes/estado em [src/lib](src/lib).

## Workflows essenciais
- Frontend dev: `npm run dev` (prepara Pyodide e sobe Vite). Vite roda no 5173; o backend permite CORS via [backend/dev.sh](backend/dev.sh).
- Backend dev (hot-reload): `backend/dev.sh` executa `uvicorn open_webui.main:app` na porta 8080.
- Build frontend: `npm run build` (sempre executa `pyodide:fetch` primeiro) — veja [package.json](package.json).
- Lint/check: `npm run lint` (eslint + svelte-check + pylint). Testes: `npm run test:frontend` e `npm run cy:open` (Cypress).

## Convenções e padrões do projeto
- Routers são importados e registrados no app principal; novas rotas devem seguir o padrão de módulos em [backend/open_webui/routers](backend/open_webui/routers).
- Config via variáveis de ambiente é normalizada em `open_webui.config`; mudanças de config devem entrar ali.
- A execução em container usa scripts de entrada; veja o comportamento de secret keys e workers em [backend/start.sh](backend/start.sh).

## Integrações importantes
- Conectores LLM (Ollama/OpenAI e compatíveis) e pipelines são configurados no backend; entradas visíveis em [backend/open_webui/main.py](backend/open_webui/main.py).
- Recursos de RAG/embeddings e ferramentas estão separados por domínio nos routers e utilitários do backend.
- Pyodide é requisito do frontend: o fetch é obrigatório em `dev`/`build` via scripts em [package.json](package.json).
