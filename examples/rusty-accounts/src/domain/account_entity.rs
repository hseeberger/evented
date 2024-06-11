use evented::entity::{Command, Entity, EventWithMetadata};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountEntity {
    #[default]
    Nonexistent,

    Existing {
        balance: u64,
    },
}

impl Entity for AccountEntity {
    type Id = Uuid;
    type Event = AccountEvent;
    type Metadata = ();

    const TYPE_NAME: &'static str = "account";

    fn handle_event(&mut self, event: Self::Event) {
        match self {
            AccountEntity::Nonexistent => match event {
                AccountEvent::Created { .. } => *self = AccountEntity::Existing { balance: 0 },

                AccountEvent::Deposited { .. } => {
                    panic!("invalid event {event:?} in state Deleted")
                }

                AccountEvent::Withdrawn { .. } => {
                    panic!("invalid event {event:?} in state Deleted")
                }
            },

            AccountEntity::Existing { .. } => match event {
                AccountEvent::Created { .. } => panic!("invalid event {event:?} in state Deleted"),

                AccountEvent::Deposited { balance, .. } => {
                    *self = AccountEntity::Existing { balance }
                }

                AccountEvent::Withdrawn { balance, .. } => {
                    *self = AccountEntity::Existing { balance }
                }
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AccountEvent {
    Created { id: Uuid },
    Deposited { id: Uuid, amount: u64, balance: u64 },
    Withdrawn { id: Uuid, amount: u64, balance: u64 },
}

// Command: CreateAccount ==========================================================================

#[derive(Debug)]
pub struct CreateAccount;

impl Command for CreateAccount {
    type Entity = AccountEntity;
    type Rejection = CreateAccountRejection;

    async fn handle(
        self,
        id: &<Self::Entity as Entity>::Id,
        entity: &Self::Entity,
    ) -> Result<
        Vec<
            impl Into<
                EventWithMetadata<
                    <Self::Entity as Entity>::Event,
                    <Self::Entity as Entity>::Metadata,
                >,
            >,
        >,
        Self::Rejection,
    > {
        let id = *id;

        match entity {
            AccountEntity::Nonexistent => {
                let event = AccountEvent::Created { id };
                Ok(vec![event])
            }

            AccountEntity::Existing { .. } => Err(CreateAccountRejection::AccountAlreadyExists(id)),
        }
    }
}

#[derive(Debug, Error)]
pub enum CreateAccountRejection {
    #[error("account with ID {0} already exists")]
    AccountAlreadyExists(Uuid),
}

// Command: Deposit ================================================================================

#[derive(Debug)]
pub struct Deposit {
    amount: u64,
}

impl From<u64> for Deposit {
    fn from(amount: u64) -> Self {
        Self { amount }
    }
}

impl Command for Deposit {
    type Entity = AccountEntity;
    type Rejection = DepositRejection;

    async fn handle(
        self,
        id: &<Self::Entity as Entity>::Id,
        entity: &Self::Entity,
    ) -> Result<
        Vec<
            impl Into<
                EventWithMetadata<
                    <Self::Entity as Entity>::Event,
                    <Self::Entity as Entity>::Metadata,
                >,
            >,
        >,
        Self::Rejection,
    > {
        let id = *id;

        match entity {
            AccountEntity::Nonexistent => Err(DepositRejection::NotFound(id)),

            AccountEntity::Existing { balance } => {
                let event = AccountEvent::Deposited {
                    id,
                    amount: self.amount,
                    balance: balance + self.amount,
                };
                Ok(vec![event])
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum DepositRejection {
    #[error("account with ID {0} not found")]
    NotFound(Uuid),
}

// Command: Withdraw ===============================================================================

#[derive(Debug)]
pub struct Withdraw {
    amount: u64,
}

impl From<u64> for Withdraw {
    fn from(amount: u64) -> Self {
        Self { amount }
    }
}

impl Command for Withdraw {
    type Entity = AccountEntity;
    type Rejection = WithdrawRejection;

    async fn handle(
        self,
        id: &<Self::Entity as Entity>::Id,
        entity: &Self::Entity,
    ) -> Result<
        Vec<
            impl Into<
                EventWithMetadata<
                    <Self::Entity as Entity>::Event,
                    <Self::Entity as Entity>::Metadata,
                >,
            >,
        >,
        Self::Rejection,
    > {
        let id = *id;

        match entity {
            AccountEntity::Nonexistent => Err(WithdrawRejection::NotFound(id)),

            AccountEntity::Existing { balance } if self.amount > *balance => {
                Err(WithdrawRejection::InsufficientBalance(id))
            }

            AccountEntity::Existing { balance } => {
                let event = AccountEvent::Withdrawn {
                    id,
                    amount: self.amount,
                    balance: balance - self.amount,
                };
                Ok(vec![event])
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum WithdrawRejection {
    #[error("account with ID {0} not found")]
    NotFound(Uuid),

    #[error("account with ID {0} has insufficient balance for withdrawal")]
    InsufficientBalance(Uuid),
}
