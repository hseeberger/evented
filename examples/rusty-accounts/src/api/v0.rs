use crate::{
    api::AppState,
    domain::{
        Account, AccountEntity, AccountRepository, CreateAccount, CreateAccountRejection, Deposit,
        DepositRejection, Withdraw, WithdrawRejection,
    },
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use error_ext::{axum::Error, StdErrorExt};
use evented::{
    entity::{EntityExt, EventSourcedEntity, NoOpEventListener},
    pool::Pool,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tracing::{error, instrument};
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    paths(list_accounts, create_accounts, deposit, withdraw),
    components(schemas(Error, ListAccountsResponse, Account, DepositRequest, WithdrawRequest))
)]
pub struct ApiDoc;

pub fn app<R>() -> Router<AppState<R>>
where
    R: AccountRepository,
{
    Router::new()
        .route("/accounts", get(list_accounts).post(create_accounts))
        .route("/accounts/:id/deposits", post(deposit))
        .route("/accounts/:id/withdrawals", post(withdraw))
}

#[derive(Debug, Serialize, ToSchema)]
struct ListAccountsResponse {
    accounts: Vec<Account>,
}

/// List accounts.
#[utoipa::path(
    get,
    path = "/accounts",
    responses(
        (status = 200, description = "A list of accounts.", body = ListAccountsResponse),
    ),
    tag = "account",
)]
#[instrument(skip(app_state))]
async fn list_accounts<R>(
    State(app_state): State<AppState<R>>,
) -> Result<Json<ListAccountsResponse>, Error>
where
    R: AccountRepository,
{
    let accounts = app_state
        .account_repository
        .accounts()
        .await
        .map_err(|error| {
            error!(error = error.as_chain(), "cannot list accounts");
            Error::Internal
        })?;

    let accounts = accounts.try_collect::<Vec<_>>().await.map_err(|error| {
        error!(error = error.as_chain(), "cannot list accounts");
        Error::Internal
    })?;

    Ok(Json(ListAccountsResponse { accounts }))
}

/// Create an account.
#[utoipa::path(
    post,
    path = "/accounts",
    responses(
        (status = 201, description = "The created account", body = Account),
        (status = 409, description = "An account with the created ID already exists", body = Error),
    ),
    tag = "account",
)]
#[instrument(skip(app_state))]
async fn create_accounts<R>(
    State(app_state): State<AppState<R>>,
) -> Result<(StatusCode, Json<Account>), Error>
where
    R: AccountRepository,
{
    let id = Uuid::now_v7();
    let mut account = spawn_account_entity(id, app_state.pool.clone()).await?;
    account
        .handle_command(CreateAccount)
        .await
        .map_err(|error| {
            error!(
                error = error.as_chain(),
                "cannot handle CreateAccount command"
            );
            Error::Internal
        })?
        .map_err(|error| match error {
            CreateAccountRejection::AccountAlreadyExists(_) => Error::conflict(error),
        })
        .map(|account| match account {
            AccountEntity::Nonexistent => panic!("invalid entity {account:?}"),

            AccountEntity::Existing { balance } => {
                let account = Account {
                    id,
                    balance: *balance,
                };
                (StatusCode::CREATED, Json(account))
            }
        })
}

#[derive(Debug, Deserialize, ToSchema)]
struct DepositRequest {
    amount: u64,
}

/// Creates a deposit.
#[utoipa::path(
    post,
    path = "/accounts/{id}/deposits",
    responses(
        (status = 200, description = "The updated account", body = Account),
        (status = 404, description = "An account with the given ID cannot be found", body = Error),
    ),
    tag = "account",
)]
#[instrument(skip(app_state))]
async fn deposit<R>(
    State(app_state): State<AppState<R>>,
    Path(id): Path<Uuid>,
    Json(DepositRequest { amount }): Json<DepositRequest>,
) -> Result<Json<Account>, Error>
where
    R: AccountRepository,
{
    let mut account = spawn_account_entity(id, app_state.pool).await?;
    account
        .handle_command(Deposit::from(amount))
        .await
        .map_err(|error| {
            error!(
                error = error.as_chain(),
                "cannot handle CreateAccount command"
            );
            Error::Internal
        })?
        .map_err(|error| match error {
            DepositRejection::NotFound(_) => Error::not_found(error),
        })
        .map(|account| match account {
            AccountEntity::Nonexistent => panic!("invalid entity {account:?}"),

            AccountEntity::Existing { balance } => {
                let account = Account {
                    id,
                    balance: *balance,
                };
                Json(account)
            }
        })
}

#[derive(Debug, Deserialize, ToSchema)]
struct WithdrawRequest {
    amount: u64,
}

/// Creates a withdrawal.
#[utoipa::path(
    post,
    path = "/accounts/{id}/withdrawals",
    responses(
        (status = 200, description = "The updated account", body = Account),
        (status = 404, description = "An account with the given ID cannot be found", body = Error),
        (status = 422, description = "An account with the given ID cannot be found", body = Error),
    ),
    tag = "account",
)]
#[instrument(skip(app_state))]
async fn withdraw<R>(
    State(app_state): State<AppState<R>>,
    Path(id): Path<Uuid>,
    Json(WithdrawRequest { amount }): Json<WithdrawRequest>,
) -> Result<Json<Account>, Error>
where
    R: AccountRepository,
{
    let mut account = spawn_account_entity(id, app_state.pool.clone()).await?;
    account
        .handle_command(Withdraw::from(amount))
        .await
        .map_err(|error| {
            error!(
                error = error.as_chain(),
                "cannot handle CreateAccount command"
            );
            Error::Internal
        })?
        .map_err(|error| match error {
            WithdrawRejection::NotFound(_) => Error::not_found(error),
            WithdrawRejection::InsufficientBalance(_) => Error::invalid_entity(error),
        })
        .map(|account| match account {
            AccountEntity::Nonexistent => panic!("invalid entity {account:?}"),

            AccountEntity::Existing { balance } => {
                let account = Account {
                    id,
                    balance: *balance,
                };
                Json(account)
            }
        })
}

// In the real-world, entities would be cached.
async fn spawn_account_entity(
    id: Uuid,
    pool: Pool,
) -> Result<EventSourcedEntity<AccountEntity, NoOpEventListener>, Error> {
    AccountEntity::default()
        .entity()
        .build(id, pool)
        .await
        .map_err(|error| {
            error!(error = error.as_chain(), "cannot build AccountEntity");
            Error::Internal
        })
}
