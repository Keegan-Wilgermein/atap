//! # Handle Set Tuples
//! Tuples of sets as sets, from one element to twelve
//!
//! Written once as a macro, since each arity is the same code with
//! one more element. Twelve matches the standard library, and a
//! larger set nests tuples inside tuples

use crate::modules::{
    gather::{Access, Gather, access},
    handle_set::{HandleSet, sealed},
};
use std::sync::Arc;

/// Makes a tuple of sets a set, handing on a tuple of their outputs
macro_rules! tuple_sets {
    ($($name:ident $index:tt),+) => {
        impl<$($name),+> sealed::Sealed for ($($name,)+) {}

        impl<$($name),+> HandleSet for ($($name,)+)
        where
            $($name: HandleSet,)+
        {
            type Output = ($($name::Output,)+);
            type Slots = ($($name::Slots,)+);

            fn slots(&self) -> Self::Slots {
                ($(self.$index.slots(),)+)
            }

            fn filled(slots: &Self::Slots) -> bool {
                true $(&& $name::filled(&slots.$index))+
            }

            fn assemble(slots: &mut Self::Slots) -> Self::Output {
                ($($name::assemble(&mut slots.$index),)+)
            }

            fn link<Root>(
                self,
                gather: &Arc<Gather<Root>>,
                outer: Access<Root::Slots, Self::Slots>,
                held: &mut Vec<Box<dyn Send>>,
            )
            where
                Root: HandleSet,
            {
                $(
                    let reach = Arc::clone(&outer);

                    self.$index.link(
                        gather,
                        access(move |root: &mut Root::Slots| &mut reach(root).$index),
                        held,
                    );
                )+
            }
        }
    };
}

tuple_sets!(A 0);
tuple_sets!(A 0, B 1);
tuple_sets!(A 0, B 1, C 2);
tuple_sets!(A 0, B 1, C 2, D 3);
tuple_sets!(A 0, B 1, C 2, D 3, E 4);
tuple_sets!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);
