//! # Merge Set Tuples
//! Tuples of merge sets as merge sets, from one element to twelve
//!
//! Every element has to give the same thing, which the first one
//! decides

use crate::modules::{handle_kind::Waiting, merge_set::MergeSet, task_handle::TaskHandle};

/// Makes a tuple of merge sets a merge set
macro_rules! merge_sets {
    ($first:ident $first_index:tt $(, $name:ident $index:tt)*) => {
        impl<Value, Marker, $first $(, $name)*> MergeSet<Value, Marker> for ($first, $($name,)*)
        where
            $first: MergeSet<Value, Marker>,
            $($name: MergeSet<Value, Marker, Given = $first::Given>,)*
        {
            type Given = $first::Given;

            fn link(self, target: &TaskHandle<(), Waiting<Self::Given>>, held: &mut Vec<Box<dyn Send>>) {
                self.$first_index.link(target, held);
                $(self.$index.link(target, held);)*
            }
        }
    };
}

merge_sets!(A 0);
merge_sets!(A 0, B 1);
merge_sets!(A 0, B 1, C 2);
merge_sets!(A 0, B 1, C 2, D 3);
merge_sets!(A 0, B 1, C 2, D 3, E 4);
merge_sets!(A 0, B 1, C 2, D 3, E 4, F 5);
merge_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
merge_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
merge_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
merge_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
merge_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
merge_sets!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);
