import itertools
from string import Formatter


class RemakeFormatter(Formatter):
    """No automatic/integer numbering allowed, missing values allowed"""
    def format(self, format_string, /, **kwargs):
        # no *args
        parsed_list = self.parse(format_string)
        for literal_text, field_name, format_spec, conversion in \
                parsed_list:
            if field_name is None:
                continue
            if field_name == '':
                raise ValueError('automatic field numbering not allowed')
            elif field_name.isdigit():
                raise ValueError('integer field numbering not allowed')

        return self.vformat(format_string, tuple(), kwargs)

    def _vformat(self, format_string, args, kwargs, used_args, recursion_depth,
                 auto_arg_index=0):
        if recursion_depth < 0:
            raise ValueError('Max string recursion exceeded')
        result = []
        # modified so I can access each term again.
        parsed_list = list(self.parse(format_string))
        for i, (literal_text, field_name, format_spec, conversion) in \
                enumerate(parsed_list):

            # output the literal text
            if literal_text:
                result.append(literal_text)

            # if there's a field, output it
            if field_name is not None:
                # this is some markup, find the object and do
                #  the formatting

                # handle arg indexing when empty field_names are given.
                if field_name == '':
                    if auto_arg_index is False:
                        raise ValueError('cannot switch from manual field '
                                         'specification to automatic field '
                                         'numbering')
                    field_name = str(auto_arg_index)
                    auto_arg_index += 1
                elif field_name.isdigit():
                    if auto_arg_index:
                        raise ValueError('cannot switch from manual field '
                                         'specification to automatic field '
                                         'numbering')
                    # disable auto arg incrementing, if it gets
                    # used later on, then an exception will be raised
                    auto_arg_index = False

                # given the field_name, find the object it references
                #  and the argument it came from
                try:
                    obj, arg_used = self.get_field(field_name, args, kwargs)
                    used_args.add(arg_used)
                except (IndexError, KeyError):
                    obj, arg_used = None, None

                if obj is not None:
                    # do any conversion on the resulting object
                    obj = self.convert_field(obj, conversion)

                    # expand the format spec, if needed
                    format_spec, auto_arg_index = self._vformat(
                        format_spec, args, kwargs,
                        used_args, recursion_depth-1,
                        auto_arg_index=auto_arg_index)

                    # format the object and append to the result
                    result.append(self.format_field(obj, format_spec))
                else:
                    result.append(self.build_term(parsed_list[i]))

        return ''.join(result), auto_arg_index

    # from a parsed_term, build the string that it was parsed from.
    def build_term(self, term):
        # (literal_text, field_name, format_spec, conversion)
        # literal_text = t[0]
        field_name = term[1]
        conversion = '!' + term[3] if term[3] else ''
        format_spec = ':' + term[2] if term[2] else ''
        if field_name is None:
            return ''
        else:
            return '{' + field_name + conversion + format_spec + '}'


formatter = RemakeFormatter()


def remake_format(s, **kwargs):
    return formatter.format(s, **kwargs)


def remake_dict_format(d, **kwargs):
    return dict([(remake_format(k, **kwargs), remake_format(v, **kwargs)) for k, v in d.items()])


def remake_dict_expand(d, **iterdict):
    result = {}
    for vals in itertools.product(*iterdict.values()):
        kwargs = dict(zip(iterdict.keys(), vals))
        result.update(remake_dict_format(d, **kwargs))
    return result
