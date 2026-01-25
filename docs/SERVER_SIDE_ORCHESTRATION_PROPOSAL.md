# Proposta: Server-Side Orchestration (SSO)

## 1) Problema
A orquestração server-side (SSO) está implementada no backend e retorna **202 Accepted** com `task_id`. Porém, o frontend atual espera **streaming SSE** ou uma resposta JSON imediata do endpoint de chat. Isso causa a falha percebida como “Erro ao fazer requisição” quando a orquestração é ativada.

## 2) Motivo
A arquitetura atual do chat assume uma resposta síncrona (ou streaming) para renderização em tempo real. A orquestração, por outro lado, devolve imediatamente o controle ao cliente e move a geração para tarefas em background. Sem **tratamento do 202**, **canal de acompanhamento**, e **persistência de estado**, o cliente fica sem resposta consumível.

## 3) Proposta de Correção
### 3.1 Objetivo
Permitir uso estável da orquestração sem quebrar o fluxo de chat, garantindo:
- Continuidade do fluxo de mensagens (mesmo com tarefas em background).
- Observabilidade do estado (fila, em execução, concluído, falha).
- Fallback automático quando SSO não estiver disponível.

### 3.2 Estratégia (alto nível)
1. **Frontend passa a tratar 202**:
  - Quando `POST /api/chat/completions` (ou `/api/v1/chat/completions`) retorna 202, o cliente entra no estado “Processando” e passa a acompanhar a execução por **WebSocket**; **polling** é apenas fallback.
  - **Contrato mínimo de resposta 202**: `{ status: "processing", task_id, chat_id, message_id }`.
2. **Endpoint de status de tarefa**:
   - Criar `/api/v1/tasks/{task_id}` para consultar status/resultados.
3. **Persistência de resultado final**:
  - MVP: salvar status + erro final.
  - Completo: salvar resposta gerada para recuperação mesmo se a conexão for interrompida.
4. **Fallback automático**:
   - Se o canal de acompanhamento falhar (WS/Redis indisponível), o cliente volta ao fluxo síncrono/streaming, evitando quebra de UX.

## 4) Onde exatamente corrigir
### 4.1 Backend
- **Flag e configuração**
  - [backend/open_webui/env.py](backend/open_webui/env.py)
  - [backend/open_webui/config.py](backend/open_webui/config.py)

- **Orquestração atual**
  - [backend/open_webui/utils/chat.py](backend/open_webui/utils/chat.py)
    - Função `generate_chat_completion` (retorna 202 e `task_id`)

- **Gerenciamento de tarefas**
  - [backend/open_webui/tasks.py](backend/open_webui/tasks.py)
    - Criar status e persistência da tarefa

- **Nova rota de status**
  - **Adicionar no router existente** [backend/open_webui/routers/tasks.py](backend/open_webui/routers/tasks.py)
  - Router já registrado em [backend/open_webui/main.py](backend/open_webui/main.py)

### 4.2 Frontend
- **Tratamento do 202**
  - [src/lib/apis/openai/index.ts](src/lib/apis/openai/index.ts)
    - Ajustar `generateOpenAIChatCompletion` para tratar 202 e devolver `task_id`

- **Fluxo de chat**
  - [src/lib/components/chat/Chat.svelte](src/lib/components/chat/Chat.svelte)
    - Ao receber 202, iniciar acompanhamento por WS/polling

- **Streaming e eventos**
  - [src/lib/apis/streaming/index.ts](src/lib/apis/streaming/index.ts)
    - Suporte a atualização assíncrona de mensagens

## 5) Escopo de Entregas
1. **Backend**:
   - Endpoint `/api/v1/tasks/{task_id}`
   - Persistência de estado (status, erro, resultado)
   - Controle de expiração/cancelamento

2. **Frontend**:
   - Detecção e manejo de 202
   - Exibição de estado “Processando”
   - Reconexão / fallback

## 6) Riscos e Mitigações
- **WS instável/Redis ausente** → fallback automático para fluxo síncrono.
- **Perda de estado em reinício** → persistência em banco/redis.
- **UX confusa** → feedback visual claro (spinner/status).

## 7) Edge Cases e Casos de Correção
- **Cliente fecha a aba no meio da geração** → tarefa continua no backend; ao reabrir, recuperar por `chat_id` e retomar status.
- **Duplo clique/envio duplicado** → retornar 409 se já existir tarefa ativa para o mesmo `chat_id` (já há esqueleto), e exibir mensagem amigável no frontend.
- **Tarefa presa (timeout do provedor)** → aplicar timeout por tarefa e marcar como `failed` com erro de timeout.
- **Conexão WS cai** → alternar automaticamente para polling do endpoint de status.
- **Redis indisponível** → degradar para orquestração local (in‑memory) ou desabilitar SSO automaticamente.
- **Modelo retorna erro 4xx/5xx** → persistir erro final e expor no status para exibir ao usuário.
- **Multi‑aba/usuário** → sincronizar status no store do chat para evitar mensagens duplicadas.
- **Chat local (`local:`)** → não orquestrar; manter fluxo normal.
- **Requisições com arquivos grandes** → persistir metadados de arquivo e validar validade/expiração antes de retomar.
- **Reexecução manual** → botão “Tentar novamente” reaproveita payload salvo.
- **Sem permissões** → negar acesso ao status se usuário não for dono do chat.
- **Modelo removido** → sinalizar erro claro “modelo indisponível” e limpar tarefa.
- **Upgrade/restart do backend** → restaurar tasks em execução via Redis/DB e marcar tasks órfãs.
- **Mensagens com ferramentas** → persistir estado intermediário de tool calls para replay seguro.

## 8) Melhorias para o Usuário (UX)
- **Status visível**: “Em fila”, “Processando”, “Concluído”, “Falhou”.
- **Estimativa simples**: tempo decorrido + número de tentativas.
- **Botão “Cancelar”**: chama endpoint de cancelamento e encerra a tarefa.
- **Reabrir chat**: se há tarefa ativa, mostrar banner “Processando em segundo plano”.
- **Notificações**: toast ao concluir/falhar.
- **Histórico resiliente**: mensagem parcial mantida com indicação de “gerando”.
- **Modo offline**: informar perda de conexão e retomar automaticamente quando voltar.
- **Painel de tarefas**: lista de gerações ativas com opção de cancelar.
- **Acessibilidade**: estados com aria‑live e feedback sonoro opcional.

## 9) Melhorias Técnicas
- **Persistência de tarefa**: salvar status, timestamps, erro e resultado (ex.: `tasks` table ou Redis + snapshot em DB).
- **Timeout por tarefa**: TTL configurável; limpar tarefas antigas.
- **Idempotência**: chave determinística por `chat_id + message_id` para evitar duplicação.
- **Observabilidade**: logs estruturados + métricas (tempo de fila, duração, taxa de falha).
- **Backpressure**: limitar tarefas simultâneas por usuário/modelo (requisito operacional).
- **Circuit breaker**: pausar orquestração se o provedor estiver instável.
- **Rate limit adaptativo**: reduzir concorrência quando erro 429 aumentar.
- **Snapshot de payload**: versão do modelo/parâmetros anexada à tarefa.
- **Compatibilidade com streaming**: reidratar mensagens parciais ao reconectar.

## 10) Segurança e Compliance
- **Autorização**: endpoint de status deve validar se o usuário possui o `chat_id`.
- **Sanitização**: nunca retornar payloads sensíveis no status.
- **Rate limits**: evitar polling agressivo no frontend.
- **Audit trail**: registrar início/fim/erro por tarefa com `user_id`.
- **Retenção**: política de expiração de resultados e logs.
- **Privacy**: evitar persistir conteúdos sensíveis quando `private_mode` ativo.

## 11) Proposta de Integração (Caminho Feliz)
1. `POST /api/chat/completions` (ou `/api/v1/chat/completions`) → 202 + `task_id`.
2. Frontend inicia acompanhamento por WS; se falhar, polling em `/api/v1/tasks/{task_id}`.
3. Backend atualiza status e emite eventos conforme progresso.
4. Resultado final é persistido e exibido no chat.

## 12) Próximos Passos
1. Validar escopo mínimo com time/usuário.
2. Implementar endpoint de status.
3. Ajustar frontend para 202 e status.
4. Testes em ambiente com e sem Redis.

## 12.1) Referências por Linha (pontos de alteração)
> Os links abaixo apontam para os trechos atuais onde as alterações devem ocorrer.

### Backend
- Flag SSO (env): [backend/open_webui/env.py](backend/open_webui/env.py#L362-L365)
- Flag SSO (config persistente): [backend/open_webui/config.py](backend/open_webui/config.py#L1589-L1594)
- Orquestração no chat (retorno 202): [backend/open_webui/utils/chat.py](backend/open_webui/utils/chat.py#L341-L382)
- Endpoint principal de chat (202 + task_id): [backend/open_webui/main.py](backend/open_webui/main.py#L1563-L1576)
- Router de tasks existente (adicionar status/cancel): [backend/open_webui/routers/tasks.py](backend/open_webui/routers/tasks.py#L42-L120)
- Infra de tasks (estado/Redis): [backend/open_webui/tasks.py](backend/open_webui/tasks.py#L96-L120)

### Frontend
- Tratamento de 202 na API de chat: [src/lib/apis/openai/index.ts](src/lib/apis/openai/index.ts#L362-L387)
- Fluxo de envio do chat: [src/lib/components/chat/Chat.svelte](src/lib/components/chat/Chat.svelte#L2004-L2032)
- Streaming SSE helper (função completa): [src/lib/apis/streaming/index.ts](src/lib/apis/streaming/index.ts#L26-L41)

## 13) Checklist de Implementação
### Backend
- [x] Persistir tasks (status, erro, resultado, timestamps).
- [x] Endpoint `GET /api/v1/tasks/status/{task_id}` com autorização.
- [x] Endpoint `POST /api/v1/tasks/status/{task_id}/cancel`.
- [x] Timeout/TTL por task + limpeza automática.
- [x] Registro de logs estruturados e métricas.

### Frontend
- [x] Tratar `202 Accepted` no chat e iniciar acompanhamento.
- [x] WS com fallback para polling.
- [x] UI de status (fila/processando/concluído/falhou).
- [x] Botão "Cancelar" e estado "Tentar novamente".
- [x] Banner ao reabrir chat com task ativa.

### Segurança & Confiabilidade
- [x] Validação de ownership do `chat_id`.
- [ ] Rate limit de polling.
- [ ] Política de retenção de resultados.
- [ ] Modo privado: não persistir conteúdo sensível.

## 14) Checklist de Testes
- [ ] Ativar SSO com Redis e validar fluxo completo.
- [ ] Desligar Redis e validar fallback.
- [ ] Queda de WebSocket e recuperação via polling.
- [ ] Reinício do backend durante task ativa.
- [ ] Erro 4xx/5xx do provedor e exibição correta no UI.
- [ ] Cancelamento de task em andamento.
- [ ] Multi‑aba com mesma conversa.

---

**Observação:** Esta proposta separa o planejamento da execução para evitar regressões na UX. O objetivo é introduzir SSO de forma controlada, garantindo compatibilidade com o fluxo atual.
