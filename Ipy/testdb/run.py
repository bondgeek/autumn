
from autumn.Ipy.testdb.models import Author, Book, Transaction, autumnTestConn

import unittest
import datetime

class TestModels(unittest.TestCase):
    def test_model(self):
    
        # Test Creation
        bolano = Author(1, "Roberto", "Bolano", "Chilean writer")
        bolano.save()

        james = Author(first_name='James', last_name='Joyce')
        james.save()
        
        kurt = Author(first_name='Kurt', last_name='Vonnegut', bio="sci-fi")
        kurt.save()
        
        tom = Author(first_name='Tom', last_name='Robbins')
        tom.save()

        Book(title='Ulysses', author_id=james.id).save()
        Book(title='Slaughter-House Five', author_id=kurt.id).save()
        Book(title='Jitterbug Perfume', author_id=tom.id).save()
        slww = Book(title='Still Life with Woodpecker', author_id=tom.id)
        slww.save()
        
        # Test ForeignKey
        self.assertEqual(slww.author.first_name, 'Tom')
        
        # Test OneToMany
        self.assertEqual(len(list(tom.books)), 2)
        
        kid = kurt.id
        del(james, kurt, tom, slww)
        
        # Test retrieval
        b = Book.get(title='Ulysses')[0]
        
        a = Author.get(id=b.author_id)[0]
        self.assertEqual(a.id, b.author_id)
        
        a = Author.get(id=b.id)[:]
        self.assert_(isinstance(a, list))
        
        # Test update
        new_last_name = 'Vonnegut, Jr.'
        a = Author.get(id=kid)[0]
        a.last_name = new_last_name
        a.save()
        
        a = Author.get(kid)
        self.assertEqual(a.last_name, new_last_name)
        
        # Test count
        self.assertEqual(Author.get().count(), 4)
        
        self.assertEqual(len(Book.get()[1:4]), 3)
        
        # Test delete
        a.delete()
        self.assertEqual(Author.get().count(), 3)

if __name__ == '__main__':
    unittest.main()

